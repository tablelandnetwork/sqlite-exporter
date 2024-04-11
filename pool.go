package main

import (
	"context"
	"log"
	"sync"
)

type Task interface {
	Execute(context.Context, int) error
}

type Pool struct {
	size  int
	wg    sync.WaitGroup
	tasks chan Task
}

func NewPool(sz int, maxTasks int) *Pool {
	return &Pool{
		size:  sz,
		tasks: make(chan Task, maxTasks),
	}
}

func (p *Pool) AddTask(task Task) {
	p.tasks <- task
}

func (p *Pool) Start(ctx context.Context) {
	for w := 1; w <= p.size; w++ {
		p.wg.Add(1)
		go p.run(ctx, w)
	}
}

func (p *Pool) Close() {
	close(p.tasks)
	p.wg.Wait()
}

func (p *Pool) run(ctx context.Context, worker int) {
	defer p.wg.Done()

	for task := range p.tasks {
		if err := task.Execute(ctx, worker); err != nil {
			log.Print(err)
		}
	}
}
