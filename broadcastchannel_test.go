package broadcastchannel

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestChannel(t *testing.T) {
	ctx, ctxCancel := context.WithCancel(context.Background())
	tl := Testlogger{}
	testchan := NewChannel[string](ctx, tl).WithReceiveChannelTimeout(time.Millisecond * 200)
	assert.NoError(t, testchan.Publish("Hello world"))
	sub1 := make(chan string)
	sub2 := make(chan string)

	sub1Id, _ := testchan.Subscribe(sub1)
	sub2Id, _ := testchan.Subscribe(sub2)
	var valChan1, valChan2 string
	go func() {
		for {
			valChan1 = <-sub1
			fmt.Println("Sub1 got message: " + valChan1)
		}
	}()

	go func() {
		for {
			valChan2 = <-sub2
			fmt.Println("Sub2 got message: " + valChan2)
		}
	}()
	testchan.Publish("Test1")
	time.Sleep(time.Millisecond * 100)
	assert.EqualValues(t, "Test1", valChan1)
	assert.EqualValues(t, "Test1", valChan2)
	testchan.Unsubscribe(sub2Id)
	testchan.Publish("Test2")
	time.Sleep(time.Millisecond * 100)
	assert.EqualValues(t, "Test2", valChan1)
	assert.EqualValues(t, "Test1", valChan2)

	blockingChan := make(chan string, 0)
	testchan.Subscribe(blockingChan)

	testchan.Publish("Test3")
	assert.EqualValues(t, "Test3", <-blockingChan)
	testchan.Publish("Test4")
	time.Sleep(time.Millisecond * 100)
	assert.EqualValues(t, "Test4", valChan1)
	testchan.Unsubscribe(sub1Id)

	time.Sleep(time.Second)

	ctxCancel()
	assert.Error(t, testchan.Publish("Test3"))
	_, suberr := testchan.Subscribe(sub2)
	assert.Error(t, suberr)

	testchan2 := NewChannel[string](context.Background(), tl)
	chan99 := make(chan string, 0)
	testchan2.Subscribe(chan99)
	testchan2.Publish("Hello")
	assert.EqualValues(t, "Hello", <-chan99)

}

type Testlogger struct {
}

func (t Testlogger) Debugf(s string, a ...any) {
	fmt.Println(fmt.Sprintf(s, a...))
}
