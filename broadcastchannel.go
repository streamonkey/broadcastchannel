package broadcastchannel

import (
	"context"
	"github.com/google/uuid"
	"sync"
	"time"
)

type Channel[T any] struct {
	input          chan T
	output         []*OutputChannel[T]
	outputLock     sync.RWMutex
	context        context.Context
	logger         Logger
	receiveTimeout *time.Duration
}

type OutputChannel[T any] struct {
	id      uuid.UUID
	channel chan T
}

func NewChannel[T any](ctx context.Context, logger Logger) *Channel[T] {
	var channel Channel[T]
	//Blocking channel
	channel.input = make(chan T, 0)
	channel.output = make([]*OutputChannel[T], 0)
	channel.context = ctx
	if logger != nil {
		channel.logger = logger
	}
	go channel.broadcast()
	return &channel
}

func (ch *Channel[T]) Publish(val T) error {
	err := ch.context.Err()
	if err != nil {
		return err
	}
	if ch.logger != nil {
		ch.logger.Debugf("Publishing %v", val)
	}
	ch.input <- val
	return nil
}

func (ch *Channel[T]) WithReceiveChannelTimeout(timeout time.Duration) *Channel[T] {
	ch.receiveTimeout = &timeout
	return ch
}

func (ch *Channel[T]) Subscribe(subscriber chan T) (uuid.UUID, error) {
	err := ch.context.Err()
	if err != nil {
		return uuid.Nil, err
	}
	ch.outputLock.Lock()
	uid := uuid.New()
	ch.output = append(ch.output, &OutputChannel[T]{channel: subscriber, id: uid})
	if ch.logger != nil {
		ch.logger.Debugf("New channel subscriber: %s", uid)
	}
	ch.outputLock.Unlock()
	return uid, nil
}

func (ch *Channel[T]) Unsubscribe(subscriptionId uuid.UUID) {
	if ch.logger != nil {
		ch.logger.Debugf("Remove channel subscriber: %s", subscriptionId.String())
	}
	ch.outputLock.Lock()
	newOutput := make([]*OutputChannel[T], 0, len(ch.output)-1)
	for _, sub := range ch.output {
		if sub.id != subscriptionId {
			newOutput = append(newOutput, sub)
		}
	}
	ch.output = newOutput
	ch.outputLock.Unlock()
}

func (ch *Channel[T]) broadcast() {
	for {
		select {
		case val := <-ch.input:
			ch.outputLock.RLock()
			for _, outputChan := range ch.output {
				go ch.send(outputChan, val)
			}
			ch.outputLock.RUnlock()
		case <-ch.context.Done():
			if ch.logger != nil {
				ch.logger.Debugf("Main context: %s", ch.context.Err())
			}
			ch.outputLock.Lock()
			for _, outputChan := range ch.output {
				close(outputChan.channel)
			}
			ch.outputLock.Unlock()
			return
		}
	}
}

func (ch *Channel[T]) send(channel *OutputChannel[T], value T) {
	var ctx context.Context
	if ch.receiveTimeout != nil {
		ictx, _ := context.WithTimeout(ch.context, *ch.receiveTimeout)
		ctx = ictx
	} else {
		ctx = ch.context
	}
	select {
	case channel.channel <- value:
	case <-ctx.Done():
		if ch.logger != nil {
			ch.logger.Debugf("Could not deliver message to receiver %s: %s, channel blocking?", channel.id.String(), ctx.Err())
		}
	}
}
