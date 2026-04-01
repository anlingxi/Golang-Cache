// Package circuitbreaker 实现了线程安全的三状态熔断器。
//
// 状态机：
//
//	          失败次数 >= threshold
//	  Closed ─────────────────────► Open
//	    ▲                             │
//	    │  探测成功                    │ 冷却时间到
//	    │           Half-Open ◄───────┘
//	    └──────────────────────
//	       (探测失败 → 重回 Open)
//
// 使用方式：
//
//	b := circuitbreaker.New("peer:8080", circuitbreaker.DefaultOptions())
//	if err := b.Allow(); err != nil {
//	    // 熔断器开路，快速失败
//	    return err
//	}
//	err = doRequest()
//	b.Report(err == nil)
package circuitbreaker

import (
	"fmt"
	"sync/atomic"
	"time"
)

// State 熔断器状态
type State int32

const (
	StateClosed   State = iota // 正常，请求放行，统计失败
	StateOpen                  // 熔断，请求直接拒绝（快速失败）
	StateHalfOpen              // 半开，放行少量探测请求
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "Closed"
	case StateOpen:
		return "Open"
	case StateHalfOpen:
		return "Half-Open"
	default:
		return "Unknown"
	}
}

// ErrOpen 熔断器开路时返回的错误
type ErrOpen struct {
	Name     string
	CoolDown time.Duration
}

func (e *ErrOpen) Error() string {
	return fmt.Sprintf("circuit breaker [%s] is open, cooldown=%v", e.Name, e.CoolDown)
}

// Options 熔断器配置
type Options struct {
	// Threshold 在 Window 时间内连续失败达到该次数后，熔断器从 Closed → Open
	// 默认 5
	Threshold int64

	// Window 失败计数的滑动窗口时长。超出窗口的失败会被重置。
	// 默认 10s
	Window time.Duration

	// CoolDown 熔断器 Open 状态持续多久后，自动切换到 Half-Open 允许探测。
	// 默认 30s
	CoolDown time.Duration

	// ProbeCount Half-Open 状态下，最多允许多少次探测请求。
	// 只要其中 1 次成功 → Closed；全部失败 → 重回 Open。
	// 默认 1
	ProbeCount int64
}

// DefaultOptions 返回生产推荐配置
func DefaultOptions() Options {
	return Options{
		Threshold:  5,
		Window:     10 * time.Second,
		CoolDown:   30 * time.Second,
		ProbeCount: 1,
	}
}

// Breaker 三状态熔断器，所有字段使用 atomic 操作，无锁设计。
type Breaker struct {
	name   string
	opts   Options

	// 状态相关（atomic int32，存 State）
	state int32

	// Closed 状态：失败计数与首次失败时间
	failures      int64 // 失败次数（atomic）
	windowStartAt int64 // 当前窗口开始时间（UnixNano，atomic）

	// Open 状态：进入 Open 的时间点
	openAt int64 // UnixNano，atomic

	// Half-Open 状态：已放行的探测次数
	probesInFlight int64 // atomic
}

// New 创建熔断器，name 通常填 peer 地址，用于日志和错误信息。
func New(name string, opts Options) *Breaker {
	if opts.Threshold <= 0 {
		opts.Threshold = 5
	}
	if opts.Window <= 0 {
		opts.Window = 10 * time.Second
	}
	if opts.CoolDown <= 0 {
		opts.CoolDown = 30 * time.Second
	}
	if opts.ProbeCount <= 0 {
		opts.ProbeCount = 1
	}
	return &Breaker{
		name:          name,
		opts:          opts,
		state:         int32(StateClosed),
		windowStartAt: time.Now().UnixNano(),
	}
}

// Allow 判断当前请求是否允许放行。
//
//   - Closed → 直接放行，返回 nil
//   - Open   → 检查冷却时间；未到则返回 *ErrOpen；到了则切换 Half-Open 并放行
//   - Half-Open → 若探测名额未用完则放行；否则拒绝
func (b *Breaker) Allow() error {
	switch State(atomic.LoadInt32(&b.state)) {
	case StateClosed:
		return nil

	case StateOpen:
		// 检查冷却时间是否已过
		openAt := time.Unix(0, atomic.LoadInt64(&b.openAt))
		if time.Since(openAt) < b.opts.CoolDown {
			return &ErrOpen{Name: b.name, CoolDown: b.opts.CoolDown}
		}
		// 冷却时间已过：尝试切到 Half-Open
		// 用 CAS 避免多个协程同时切换
		if atomic.CompareAndSwapInt32(&b.state, int32(StateOpen), int32(StateHalfOpen)) {
			atomic.StoreInt64(&b.probesInFlight, 0)
		}
		// 切换成功或其他协程已经切换，走 Half-Open 逻辑
		fallthrough

	case StateHalfOpen:
		// 原子地占一个探测名额
		probes := atomic.AddInt64(&b.probesInFlight, 1)
		if probes > b.opts.ProbeCount {
			// 名额已满，拒绝
			atomic.AddInt64(&b.probesInFlight, -1)
			return &ErrOpen{Name: b.name, CoolDown: b.opts.CoolDown}
		}
		return nil
	}

	return nil
}

// Report 上报请求结果。
//
//   - success=true ：请求成功
//     Closed → 重置失败计数
//     Half-Open → 切回 Closed（探测成功，节点恢复）
//   - success=false：请求失败
//     Closed → 累加失败计数；若超阈值 → 切 Open
//     Half-Open → 重回 Open（节点仍有问题）
func (b *Breaker) Report(success bool) {
	switch State(atomic.LoadInt32(&b.state)) {
	case StateClosed:
		if success {
			// 成功：重置窗口
			atomic.StoreInt64(&b.failures, 0)
			atomic.StoreInt64(&b.windowStartAt, time.Now().UnixNano())
			return
		}
		// 失败：检查是否超出窗口
		winStart := time.Unix(0, atomic.LoadInt64(&b.windowStartAt))
		if time.Since(winStart) > b.opts.Window {
			// 窗口已过，重新计数
			atomic.StoreInt64(&b.failures, 1)
			atomic.StoreInt64(&b.windowStartAt, time.Now().UnixNano())
			return
		}
		// 窗口内新增失败
		failures := atomic.AddInt64(&b.failures, 1)
		if failures >= b.opts.Threshold {
			// 达到阈值：Closed → Open
			if atomic.CompareAndSwapInt32(&b.state, int32(StateClosed), int32(StateOpen)) {
				atomic.StoreInt64(&b.openAt, time.Now().UnixNano())
			}
		}

	case StateHalfOpen:
		if success {
			// 探测成功：Half-Open → Closed，重置计数
			if atomic.CompareAndSwapInt32(&b.state, int32(StateHalfOpen), int32(StateClosed)) {
				atomic.StoreInt64(&b.failures, 0)
				atomic.StoreInt64(&b.windowStartAt, time.Now().UnixNano())
			}
		} else {
			// 探测失败：Half-Open → Open，重新计冷却时间
			if atomic.CompareAndSwapInt32(&b.state, int32(StateHalfOpen), int32(StateOpen)) {
				atomic.StoreInt64(&b.openAt, time.Now().UnixNano())
			}
		}

	case StateOpen:
		// Open 状态下不统计（请求已被 Allow 拒绝，不应该走到这里）
	}
}

// State 返回当前状态（用于监控和测试）
func (b *Breaker) State() State {
	return State(atomic.LoadInt32(&b.state))
}

// Failures 返回当前失败计数（用于监控）
func (b *Breaker) Failures() int64 {
	return atomic.LoadInt64(&b.failures)
}

// Name 返回熔断器名称
func (b *Breaker) Name() string {
	return b.name
}
