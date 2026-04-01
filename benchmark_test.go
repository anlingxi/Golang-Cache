package kamacache

// benchmark_test.go：端到端性能基准测试
//
// 运行方式：
//
//	go test -bench=. -benchmem -benchtime=3s
//
// 核心指标：
//   - ns/op：每次操作耗时（越小越好）
//   - B/op：每次操作分配内存（越小越好）
//   - allocs/op：每次操作内存分配次数（越小越好）

import (
	"context"
	"fmt"
	"testing"
)

// ---- 辅助工厂 ----

func newTestGroup(name string, cacheBytes int64, opts ...GroupOption) *Group {
	// 每次测试用不同名称，避免冲突
	getter := GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return []byte("value-" + key), nil
	})
	g := NewGroup(name, cacheBytes, getter, opts...)
	return g
}

func cleanup(name string) {
	DestroyGroup(name)
}

// ---- Get 基准测试 ----

// BenchmarkGroup_Get_CacheHit 缓存命中场景（最常见的热路径）
func BenchmarkGroup_Get_CacheHit(b *testing.B) {
	g := newTestGroup("bench-hit", 1<<20)
	defer cleanup("bench-hit")

	ctx := context.Background()
	// 预热：写入 1000 个 key
	for i := 0; i < 1000; i++ {
		g.Set(ctx, fmt.Sprintf("key-%d", i), []byte(fmt.Sprintf("value-%d", i)))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			g.Get(ctx, fmt.Sprintf("key-%d", i%1000))
			i++
		}
	})
}

// BenchmarkGroup_Get_CacheMiss_NoBloom 缓存未命中（无布隆过滤器）
// 每次 miss 都会调用 getter，getter 返回数据后加入缓存
func BenchmarkGroup_Get_CacheMiss_NoBloom(b *testing.B) {
	g := newTestGroup("bench-miss-nobloom", 1<<20)
	defer cleanup("bench-miss-nobloom")

	ctx := context.Background()
	b.ResetTimer()
	// 每次用新的 key，确保都是 miss（getter 会被调用）
	for i := 0; i < b.N; i++ {
		g.Get(ctx, fmt.Sprintf("nonexist-%d", i))
	}
}

// BenchmarkGroup_Get_CacheMiss_WithBloom 缓存未命中（有布隆过滤器）
// 布隆过滤器直接拦截，getter 不会被调用
func BenchmarkGroup_Get_CacheMiss_WithBloom(b *testing.B) {
	g := newTestGroup("bench-miss-bloom", 1<<20,
		WithBloomFilter(100000, 0.01),
	)
	defer cleanup("bench-miss-bloom")

	ctx := context.Background()
	b.ResetTimer()
	// 这些 key 从未被 Set，布隆过滤器将直接拦截
	for i := 0; i < b.N; i++ {
		g.Get(ctx, fmt.Sprintf("attack-%d", i))
	}
}

// BenchmarkGroup_Get_Mixed 混合场景：50% 命中 + 50% 攻击不存在的 key
func BenchmarkGroup_Get_Mixed_NoBloom(b *testing.B) {
	g := newTestGroup("bench-mixed-nobloom", 1<<20)
	defer cleanup("bench-mixed-nobloom")

	ctx := context.Background()
	for i := 0; i < 500; i++ {
		g.Set(ctx, fmt.Sprintf("key-%d", i), []byte("value"))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				g.Get(ctx, fmt.Sprintf("key-%d", i%500))
			} else {
				g.Get(ctx, fmt.Sprintf("attack-%d", i)) // 不存在的 key
			}
			i++
		}
	})
}

func BenchmarkGroup_Get_Mixed_WithBloom(b *testing.B) {
	g := newTestGroup("bench-mixed-bloom", 1<<20,
		WithBloomFilter(10000, 0.01),
	)
	defer cleanup("bench-mixed-bloom")

	ctx := context.Background()
	for i := 0; i < 500; i++ {
		g.Set(ctx, fmt.Sprintf("key-%d", i), []byte("value"))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				g.Get(ctx, fmt.Sprintf("key-%d", i%500))
			} else {
				g.Get(ctx, fmt.Sprintf("attack-%d", i)) // 布隆过滤器直接拦截
			}
			i++
		}
	})
}

// ---- Set 基准测试 ----

// BenchmarkGroup_Set 写入性能
func BenchmarkGroup_Set(b *testing.B) {
	g := newTestGroup("bench-set", 1<<20)
	defer cleanup("bench-set")

	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			g.Set(ctx, fmt.Sprintf("key-%d", i), []byte("value"))
			i++
		}
	})
}

// BenchmarkGroup_Set_WithBloom 带布隆过滤器的写入（多一次 bloom.Add）
func BenchmarkGroup_Set_WithBloom(b *testing.B) {
	g := newTestGroup("bench-set-bloom", 1<<20,
		WithBloomFilter(uint(b.N)+1000, 0.01),
	)
	defer cleanup("bench-set-bloom")

	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			g.Set(ctx, fmt.Sprintf("key-%d", i), []byte("value"))
			i++
		}
	})
}
