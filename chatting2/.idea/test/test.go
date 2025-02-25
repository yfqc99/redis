package main

import (
	"fmt"
	"sync"
	"time"
)

func mian() {
	fmt.Println("test")
}
func demo1() {
	//创建一个协程等待组
	wg := sync.WaitGroup{}
	//创建一个传输int类型的通道
	ch := make(chan int, 10)
	for i := 0; i < 10; i++ {
		ch <- i
	}
	close(ch)
	//登记3个协程
	wg.Add(3)
	for j := 0; j < 3; j++ {
		//可能会出现协程阻塞的情况
		//数据接收完后会一直返回当前类型的零值
		go func() {
			//for {
			//task := <-ch
			/*task, ok := <-ch
			if !ok {
				break
			}*/
			// 这里假设对接收的数据执行某些操作
			/*fmt.Println(task)
			}*/
			for {
				select {
				case task := <-ch:
					//对接收数据操作完毕后，结束当前协程
					fmt.Println(task)
				default:
					wg.Done()
				}
			}
		}()
	}
	wg.Wait()
}
func demo2() {
	//创建无缓存通道
	ch := make(chan string)
	go func() {
		// 这里假设执行一些耗时的操作
		time.Sleep(3 * time.Second)
		ch <- "job result"
	}()

	select {
	//当操作一直无法完成，超过设置的超时时间函数直接被终止
	case result := <-ch:
		fmt.Println(result)
	case <-time.After(time.Second): // 较小的超时时间
		//会导致协程未按照预期退出并销毁，导致泄露
		return
	}
}
