package bloomfilter

import (
	"fmt"
	"sync"
	"testing"
)

// ---- 单元测试 ----

// TestNew 验证参数计算是否合理
func TestNew(t *testing.T) {
	tests := []struct {
		n      uint
		fpRate float64
		minM   uint // 位数下限（粗略验证）
		maxK   uint // 哈希函数数上限
	}{
		{1000, 0.01, 8000, 10},
		{100000, 0.01, 800000, 10},
		{1000000, 0.001, 13000000, 15},
	}

	for _, tt := range tests {
		bf := New(tt.n, tt.fpRate)
		if bf.M() < tt.minM {
			t.Errorf("n=%d fpRate=%.3f: M()=%d 小于期望下限 %d", tt.n, tt.fpRate, bf.M(), tt.minM)
		}
		if bf.K() > tt.maxK {
			t.Errorf("n=%d fpRate=%.3f: K()=%d 超过期望上限 %d", tt.n, tt.fpRate, bf.K(), tt.maxK)
		}
		if bf.K() < 1 {
			t.Errorf("K 不能为 0")
		}
		t.Logf("n=%7d fpRate=%.3f → M=%d bits (%.1f KB), K=%d",
			tt.n, tt.fpRate, bf.M(), float64(bf.M())/8/1024, bf.K())
	}
}

// TestMightContain_NoFalseNegative 验证零假阴性：加入的 key 必定返回 true
func TestMightContain_NoFalseNegative(t *testing.T) {
	bf := New(10000, 0.01)

	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
		bf.Add(keys[i])
	}

	for _, key := range keys {
		if !bf.MightContain(key) {
			t.Errorf("假阴性！key %q 已加入但 MightContain 返回 false", key)
		}
	}
}

// TestMightContain_FalsePositiveRate 验证假阳性率在预期范围内
func TestMightContain_FalsePositiveRate(t *testing.T) {
	const (
		n          = 10000
		targetRate = 0.01 // 1%
		tolerance  = 2.0  // 允许实际值为目标的 2 倍（考虑统计波动）
	)

	bf := New(n, targetRate)

	// 加入 n 个 key
	for i := 0; i < n; i++ {
		bf.Add(fmt.Sprintf("exist-%d", i))
	}

	// 用另外 n 个从未加入的 key 测试假阳性
	falsePositives := 0
	trials := 100000
	for i := 0; i < trials; i++ {
		key := fmt.Sprintf("notexist-%d", i)
		if bf.MightContain(key) {
			falsePositives++
		}
	}

	actualRate := float64(falsePositives) / float64(trials)
	t.Logf("假阳性率: 目标=%.2f%%, 实际=%.2f%% (%d/%d)",
		targetRate*100, actualRate*100, falsePositives, trials)

	if actualRate > targetRate*tolerance {
		t.Errorf("假阳性率 %.4f 超过容忍上限 %.4f", actualRate, targetRate*tolerance)
	}
}

// TestMightContain_DefinitelyNotExist 验证未加入的 key 能被正确识别（统计上）
func TestMightContain_DefinitelyNotExist(t *testing.T) {
	bf := New(1000, 0.01)
	// 完全空的过滤器，任何 key 都不存在
	if bf.MightContain("anything") {
		t.Log("空过滤器对 'anything' 返回 true（极小概率的哈希冲突，可忽略）")
	}

	// 只加入特定前缀的 key，不加入 "attack-*" 前缀
	for i := 0; i < 1000; i++ {
		bf.Add(fmt.Sprintf("normal-%d", i))
	}

	// "attack-*" 的 key 绝大多数应返回 false（只有假阳性例外）
	blocked := 0
	for i := 0; i < 1000; i++ {
		if !bf.MightContain(fmt.Sprintf("attack-%d", i)) {
			blocked++
		}
	}
	t.Logf("1000 个攻击 key 中 %d 个被布隆过滤器拦截（%.1f%%）", blocked, float64(blocked)/10)

	if blocked < 950 { // 至少 95% 被拦截（1% 假阳性率下，期望 99% 被拦截）
		t.Errorf("拦截率过低：%d/1000", blocked)
	}
}

// TestConcurrentSafety 并发读写安全性测试
func TestConcurrentSafety(t *testing.T) {
	bf := New(50000, 0.01)
	var wg sync.WaitGroup

	// 并发写
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				bf.Add(fmt.Sprintf("goroutine-%d-key-%d", id, i))
			}
		}(g)
	}

	// 并发读（同时进行）
	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				bf.MightContain(fmt.Sprintf("read-key-%d-%d", id, i))
			}
		}(g)
	}

	wg.Wait()
	t.Logf("并发测试完成，填充率: %.2f%%", bf.FillRatio()*100)
}

// TestFillRatio 验证填充率计算
func TestFillRatio(t *testing.T) {
	bf := New(1000, 0.01)

	// 空过滤器填充率应为 0
	if bf.FillRatio() != 0 {
		t.Errorf("空过滤器填充率应为 0，实际 %.4f", bf.FillRatio())
	}

	// 加入元素后填充率应增加
	for i := 0; i < 1000; i++ {
		bf.Add(fmt.Sprintf("key-%d", i))
	}

	ratio := bf.FillRatio()
	t.Logf("加入 1000 个元素后填充率: %.2f%%", ratio*100)

	// 填充率应在合理范围内（理论上约为 50%，因为 k 个哈希函数被优化为此）
	if ratio < 0.3 || ratio > 0.8 {
		t.Errorf("填充率 %.4f 超出预期范围 [0.3, 0.8]", ratio)
	}
}

// TestHashDistribution 验证哈希分布的均匀性
func TestHashDistribution(t *testing.T) {
	// 两个不同的 key 应该产生不同的哈希值（大概率）
	h1a, h2a := hashKey("key-a")
	h1b, h2b := hashKey("key-b")

	if h1a == h1b && h2a == h2b {
		t.Error("不同的 key 产生了相同的哈希值")
	}

	// h2 不应为 0
	_, h2 := hashKey("test")
	if h2 == 0 {
		t.Error("h2 不应为 0")
	}
}

// ---- Benchmark ----

// BenchmarkAdd 测试写入性能
func BenchmarkAdd(b *testing.B) {
	bf := New(uint(b.N)+1000, 0.01)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			bf.Add(fmt.Sprintf("bench-key-%d", i))
			i++
		}
	})
}

// BenchmarkMightContain_Hit 测试命中场景（key 存在）的查询性能
func BenchmarkMightContain_Hit(b *testing.B) {
	const n = 100000
	bf := New(n, 0.01)
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
		bf.Add(keys[i])
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			bf.MightContain(keys[i%n])
			i++
		}
	})
}

// BenchmarkMightContain_Miss 测试未命中场景（key 不存在）的查询性能
// 这是防缓存穿透的核心路径，越快越好
func BenchmarkMightContain_Miss(b *testing.B) {
	const n = 100000
	bf := New(n, 0.01)
	for i := 0; i < n; i++ {
		bf.Add(fmt.Sprintf("exist-%d", i))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			bf.MightContain(fmt.Sprintf("notexist-%d", i))
			i++
		}
	})
}
