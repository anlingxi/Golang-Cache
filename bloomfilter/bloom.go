// Package bloomfilter 提供线程安全的布隆过滤器实现。
//
// 布隆过滤器用于防止缓存穿透：当大量请求访问不存在的 key 时，
// 布隆过滤器能在 O(k) 时间内判断 key「一定不存在」，直接拦截，
// 避免每次都打到数据库。
//
// 特性：
//   - 零外部依赖，纯标准库实现
//   - 使用 sync/atomic 保证并发安全，无锁设计
//   - 双哈希（Double Hashing）模拟多个独立哈希函数
//   - 根据预期元素数量和目标假阳性率自动计算最优参数
package bloomfilter

import (
	"math"
	"sync/atomic"
)

// BloomFilter 线程安全的布隆过滤器。
//
// 只支持 Add 和 MightContain 两个操作，不支持删除（标准布隆过滤器的限制）。
// 如需支持删除，可使用计数型布隆过滤器（Counting Bloom Filter），
// 但内存开销会增加约 4 倍。
type BloomFilter struct {
	bits []uint64 // 位图，每个 uint64 存 64 位
	k    uint     // 哈希函数个数
	m    uint     // 总位数
}

// New 根据预期元素数量 n 和目标假阳性率 fpRate 创建布隆过滤器。
//
//	n:      预期存入的最大元素数量（建议留有 20% 余量）
//	fpRate: 目标假阳性率，例如 0.01 表示 1%
//
// 内存公式：m = -n * ln(p) / (ln2)²
// 哈希数公式：k = (m/n) * ln2
//
// 示例：n=100000, fpRate=0.01 → 约 114KB，7 个哈希函数
func New(n uint, fpRate float64) *BloomFilter {
	m := optimalM(n, fpRate)
	k := optimalK(m, n)
	return &BloomFilter{
		bits: make([]uint64, (m+63)/64), // 向上取整到 64 的倍数
		k:    k,
		m:    m,
	}
}

// Add 将 key 写入布隆过滤器。
// 并发安全，多个 goroutine 可同时调用。
func (b *BloomFilter) Add(key string) {
	h1, h2 := hashKey(key)
	for i := uint(0); i < b.k; i++ {
		// 双哈希：h_i(x) = (h1 + i*h2) mod m
		pos := (h1 + uint64(i)*h2) % uint64(b.m)
		b.setBit(pos)
	}
}

// MightContain 判断 key 是否可能存在。
//
// 返回 false：key 一定不存在（零假阴性）
// 返回 true：key 可能存在，也可能是假阳性（概率约等于 fpRate）
//
// 并发安全，多个 goroutine 可同时调用。
func (b *BloomFilter) MightContain(key string) bool {
	h1, h2 := hashKey(key)
	for i := uint(0); i < b.k; i++ {
		pos := (h1 + uint64(i)*h2) % uint64(b.m)
		if !b.getBit(pos) {
			return false // 某一位为 0，key 一定不存在
		}
	}
	return true
}

// K 返回哈希函数个数（用于测试和监控）
func (b *BloomFilter) K() uint { return b.k }

// M 返回总位数（用于测试和监控）
func (b *BloomFilter) M() uint { return b.m }

// FillRatio 返回当前位图的填充率（已置 1 的位数 / 总位数）
// 填充率超过 50% 后，假阳性率会显著上升。
func (b *BloomFilter) FillRatio() float64 {
	set := uint64(0)
	for i := range b.bits {
		set += popcount(atomic.LoadUint64(&b.bits[i]))
	}
	return float64(set) / float64(b.m)
}

// ---- 内部方法 ----

// setBit 原子地将 pos 位置的 bit 置 1。
// 使用 CAS 循环，避免在 Go 1.22 不支持 atomic.OrUint64 的情况下出错。
func (b *BloomFilter) setBit(pos uint64) {
	idx := pos / 64
	mask := uint64(1) << (pos % 64)
	for {
		old := atomic.LoadUint64(&b.bits[idx])
		if old&mask != 0 {
			return // 已经是 1，无需操作
		}
		if atomic.CompareAndSwapUint64(&b.bits[idx], old, old|mask) {
			return
		}
		// CAS 失败说明有并发写入，重试
	}
}

// getBit 原子地读取 pos 位置的 bit 值。
func (b *BloomFilter) getBit(pos uint64) bool {
	idx := pos / 64
	mask := uint64(1) << (pos % 64)
	return atomic.LoadUint64(&b.bits[idx])&mask != 0
}

// hashKey 用 FNV-1a 生成 h1，再用 Murmur 混淆生成 h2。
// 双哈希技巧：用两个独立哈希值线性组合，模拟 k 个独立哈希函数。
// 论文依据：Kirsch & Mitzenmacher, "Less Hashing, Same Performance"
func hashKey(key string) (h1, h2 uint64) {
	// h1: FNV-1a 64-bit
	const (
		fnvOffset = uint64(14695981039346656037)
		fnvPrime  = uint64(1099511628211)
	)
	h1 = fnvOffset
	for i := 0; i < len(key); i++ {
		h1 ^= uint64(key[i])
		h1 *= fnvPrime
	}

	// h2: Murmur finalizer mix（让 h2 与 h1 充分独立）
	h2 = h1 ^ (h1 >> 33)
	h2 *= 0xff51afd7ed558ccd
	h2 ^= h2 >> 33
	h2 *= 0xc4ceb9fe1a85ec53
	h2 ^= h2 >> 33

	if h2 == 0 {
		h2 = 1 // h2 不能为 0，否则所有 h_i 退化成同一个哈希
	}
	return h1, h2
}

// optimalM 计算最优位数 m = ceil(-n * ln(p) / (ln2)²)
func optimalM(n uint, p float64) uint {
	if p <= 0 || p >= 1 {
		p = 0.01 // 默认 1% 假阳性率
	}
	m := math.Ceil(-float64(n) * math.Log(p) / (math.Ln2 * math.Ln2))
	if m < 64 {
		m = 64 // 最小 64 位（1 个 uint64）
	}
	return uint(m)
}

// optimalK 计算最优哈希函数数量 k = ceil((m/n) * ln2)
func optimalK(m, n uint) uint {
	if n == 0 {
		return 1
	}
	k := uint(math.Ceil(float64(m) / float64(n) * math.Ln2))
	if k < 1 {
		return 1
	}
	if k > 20 {
		return 20 // 上限 20，防止性能劣化
	}
	return k
}

// popcount 计算 uint64 中 1 的位数（Hamming Weight）
// 使用 SWAR 算法，避免循环
func popcount(x uint64) uint64 {
	x = x - ((x >> 1) & 0x5555555555555555)
	x = (x & 0x3333333333333333) + ((x >> 2) & 0x3333333333333333)
	x = (x + (x >> 4)) & 0x0f0f0f0f0f0f0f0f
	return (x * 0x0101010101010101) >> 56
}
