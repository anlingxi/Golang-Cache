package singleflight

import (
	"sync"
)

// 代表正在进行或已结束的请求
type call struct {
	// 用waitGroup来等待请求完成，确保同一时间只有一个请求在执行
	wg  sync.WaitGroup
	val interface{}
	err error
}

// Group manages all kinds of calls
type Group struct {
	m sync.Map // 使用sync.Map来优化并发性能
}

// 使用LoadOrStore来确保同一时间只有一个请求在执行，其他请求等待结果
func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	c := &call{}
	c.wg.Add(1)

	// LoadOrStore：
	// 如果 key 已存在 → 返回已存在的值，loaded=true
	// 如果 key 不存在 → 存入新值，loaded=false
	// 整个操作是原子的，不存在时间窗口
	actual, loaded := g.m.LoadOrStore(key, c)

	if loaded {
		// 有人抢先了：等他查完，直接用他的结果
		c.wg.Done() // 我自己创建的 call 用不到了，先 Done 掉（不然泄漏）
		existing := actual.(*call)
		existing.wg.Wait()
		return existing.val, existing.err
	}

	// 我是第一个：执行真正的查询
	c.val, c.err = fn()
	c.wg.Done()     // 唤醒所有等待者
	g.m.Delete(key) // 清理
	return c.val, c.err
}
