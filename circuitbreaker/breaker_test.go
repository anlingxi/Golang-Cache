package circuitbreaker

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---- 单元测试 ----

func testOpts() Options {
	return Options{
		Threshold:  3,              // 3 次失败就熔断
		Window:     5 * time.Second,
		CoolDown:   100 * time.Millisecond, // 测试用短冷却
		ProbeCount: 1,
	}
}

// TestInitialState 初始状态应为 Closed
func TestInitialState(t *testing.T) {
	b := New("test", testOpts())
	if b.State() != StateClosed {
		t.Fatalf("初始状态应为 Closed，实际为 %s", b.State())
	}
	if err := b.Allow(); err != nil {
		t.Fatalf("Closed 状态应放行请求，got err: %v", err)
	}
}

// TestClosedToOpen 连续失败达到阈值后应切换到 Open
func TestClosedToOpen(t *testing.T) {
	b := New("test", testOpts()) // threshold=3

	// 前 2 次失败：仍在 Closed
	for i := 0; i < 2; i++ {
		if err := b.Allow(); err != nil {
			t.Fatalf("第 %d 次请求应被放行", i+1)
		}
		b.Report(false)
		if b.State() != StateClosed {
			t.Fatalf("第 %d 次失败后应仍为 Closed", i+1)
		}
	}

	// 第 3 次失败：切换到 Open
	if err := b.Allow(); err != nil {
		t.Fatal("第 3 次请求（触发熔断前）应被放行")
	}
	b.Report(false)

	if b.State() != StateOpen {
		t.Fatalf("3 次失败后应切换到 Open，实际为 %s", b.State())
	}
}

// TestOpenRejectsRequests Open 状态下请求应被快速拒绝
func TestOpenRejectsRequests(t *testing.T) {
	b := New("test", testOpts())

	// 触发熔断
	for i := 0; i < 3; i++ {
		b.Allow()
		b.Report(false)
	}

	if b.State() != StateOpen {
		t.Fatal("应该处于 Open 状态")
	}

	// 后续请求应被拒绝
	for i := 0; i < 10; i++ {
		err := b.Allow()
		if err == nil {
			t.Fatal("Open 状态下请求应被拒绝")
		}
		var errOpen *ErrOpen
		if !errors.As(err, &errOpen) {
			t.Fatalf("错误类型应为 *ErrOpen，实际为 %T", err)
		}
	}
}

// TestOpenToHalfOpen 冷却时间到后应切换到 Half-Open
func TestOpenToHalfOpen(t *testing.T) {
	b := New("test", testOpts()) // cooldown=100ms

	// 触发熔断
	for i := 0; i < 3; i++ {
		b.Allow()
		b.Report(false)
	}
	if b.State() != StateOpen {
		t.Fatal("应该处于 Open 状态")
	}

	// 冷却时间内：拒绝
	if err := b.Allow(); err == nil {
		t.Fatal("冷却时间内应拒绝")
	}

	// 等待冷却时间过去
	time.Sleep(150 * time.Millisecond)

	// 现在应允许探测请求（自动切换到 Half-Open）
	if err := b.Allow(); err != nil {
		t.Fatalf("冷却后应放行探测请求，got: %v", err)
	}

	if b.State() != StateHalfOpen {
		t.Fatalf("冷却后应处于 Half-Open，实际为 %s", b.State())
	}
}

// TestHalfOpenSuccessToClose 探测成功应切回 Closed
func TestHalfOpenSuccessToClose(t *testing.T) {
	b := New("test", testOpts())

	// 触发熔断
	for i := 0; i < 3; i++ {
		b.Allow()
		b.Report(false)
	}

	// 等冷却
	time.Sleep(150 * time.Millisecond)

	// 探测放行
	if err := b.Allow(); err != nil {
		t.Fatalf("探测请求应被放行: %v", err)
	}

	// 探测成功
	b.Report(true)

	if b.State() != StateClosed {
		t.Fatalf("探测成功后应切回 Closed，实际为 %s", b.State())
	}

	// 后续请求正常放行
	if err := b.Allow(); err != nil {
		t.Fatalf("Closed 状态应放行请求: %v", err)
	}
}

// TestHalfOpenFailureToOpen 探测失败应重回 Open
func TestHalfOpenFailureToOpen(t *testing.T) {
	b := New("test", testOpts())

	// 触发熔断
	for i := 0; i < 3; i++ {
		b.Allow()
		b.Report(false)
	}

	// 等冷却
	time.Sleep(150 * time.Millisecond)

	// 探测放行
	b.Allow()

	// 探测失败
	b.Report(false)

	if b.State() != StateOpen {
		t.Fatalf("探测失败后应重回 Open，实际为 %s", b.State())
	}

	// 请求应再次被拒绝
	if err := b.Allow(); err == nil {
		t.Fatal("重回 Open 后请求应被拒绝")
	}
}

// TestWindowReset 超出窗口后，失败计数应重置
func TestWindowReset(t *testing.T) {
	opts := testOpts()
	opts.Window = 50 * time.Millisecond // 极短窗口用于测试
	b := New("test", opts)

	// 失败 2 次（低于阈值 3）
	for i := 0; i < 2; i++ {
		b.Allow()
		b.Report(false)
	}
	if b.State() != StateClosed {
		t.Fatal("2 次失败后应仍为 Closed")
	}

	// 等待窗口过期
	time.Sleep(60 * time.Millisecond)

	// 再失败 2 次（窗口已重置，计数从 1 开始）
	for i := 0; i < 2; i++ {
		b.Allow()
		b.Report(false)
	}
	if b.State() != StateClosed {
		t.Fatalf("窗口重置后 2 次失败应仍为 Closed，实际为 %s", b.State())
	}
}

// TestSuccessResetsCount 成功请求应重置失败计数
func TestSuccessResetsCount(t *testing.T) {
	b := New("test", testOpts()) // threshold=3

	// 失败 2 次
	for i := 0; i < 2; i++ {
		b.Allow()
		b.Report(false)
	}

	// 成功 1 次：重置计数
	b.Allow()
	b.Report(true)

	if b.Failures() != 0 {
		t.Fatalf("成功后失败计数应为 0，实际为 %d", b.Failures())
	}

	// 再失败 2 次：不应触发熔断（计数从 0 重新开始）
	for i := 0; i < 2; i++ {
		b.Allow()
		b.Report(false)
	}

	if b.State() != StateClosed {
		t.Fatalf("成功重置后 2 次失败应仍为 Closed，实际为 %s", b.State())
	}
}

// TestHalfOpenProbeLimit Half-Open 状态只允许 ProbeCount 个并发探测
func TestHalfOpenProbeLimit(t *testing.T) {
	opts := testOpts()
	opts.ProbeCount = 2 // 最多 2 个并发探测
	b := New("test", opts)

	// 触发熔断
	for i := 0; i < 3; i++ {
		b.Allow()
		b.Report(false)
	}

	// 等冷却
	time.Sleep(150 * time.Millisecond)

	// 前 2 个请求放行
	if err := b.Allow(); err != nil {
		t.Fatalf("第 1 个探测应放行: %v", err)
	}
	if err := b.Allow(); err != nil {
		t.Fatalf("第 2 个探测应放行: %v", err)
	}

	// 第 3 个请求应被拒绝（名额已满）
	if err := b.Allow(); err == nil {
		t.Fatal("第 3 个探测应被拒绝（超出 ProbeCount）")
	}
}

// TestConcurrentSafety 并发安全测试
func TestConcurrentSafety(t *testing.T) {
	b := New("test", Options{
		Threshold:  100,
		Window:     10 * time.Second,
		CoolDown:   10 * time.Millisecond,
		ProbeCount: 5,
	})

	var wg sync.WaitGroup
	var totalAllowed int64

	// 100 个 goroutine 并发操作
	for g := 0; g < 100; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				if err := b.Allow(); err == nil {
					atomic.AddInt64(&totalAllowed, 1)
					// 奇数 id 失败，偶数 id 成功
					b.Report(id%2 == 0)
				}
			}
		}(g)
	}

	wg.Wait()
	t.Logf("并发测试完成：总放行 %d 次，最终状态 %s，失败数 %d",
		totalAllowed, b.State(), b.Failures())
}

// TestErrOpenFormat 验证错误信息格式
func TestErrOpenFormat(t *testing.T) {
	b := New("peer:8080", testOpts())

	for i := 0; i < 3; i++ {
		b.Allow()
		b.Report(false)
	}

	err := b.Allow()
	if err == nil {
		t.Fatal("Open 状态应返回错误")
	}

	errMsg := err.Error()
	t.Logf("错误信息: %s", errMsg)

	if errMsg == "" {
		t.Fatal("错误信息不应为空")
	}
}

// ---- Benchmark ----

// BenchmarkAllow_Closed Closed 状态（热路径）的性能
func BenchmarkAllow_Closed(b *testing.B) {
	br := New("bench", DefaultOptions())
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			br.Allow()
			br.Report(true)
		}
	})
}

// BenchmarkAllow_Open Open 状态的快速失败性能
func BenchmarkAllow_Open(b *testing.B) {
	br := New("bench", Options{Threshold: 1, Window: time.Minute, CoolDown: time.Hour, ProbeCount: 1})
	br.Allow()
	br.Report(false) // 触发熔断

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			br.Allow() // 应该快速返回 ErrOpen
		}
	})
}
