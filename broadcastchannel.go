package broadcastchannel

import (
	"context"
	"github.com/google/uuid"
	"sync"
	"time"
)

// Broadcastchannel is a pub/sub channel. All input is replicated to all outputs.
type Broadcastchannel[T any] struct {
	input          chan T
	output         []*outputChannel[T]
	outputLock     sync.RWMutex
	context        context.Context
	logger         Logger
	receiveTimeout *time.Duration
}

type outputChannel[T any] struct {
	id      uuid.UUID
	channel chan T
}

// NewChannel creates a new broadcast channel, given ctx as Context for cancellation.
// logger may be nil
func NewChannel[T any](ctx context.Context, logger Logger) *Broadcastchannel[T] {
	var channel Broadcastchannel[T]
	//Blocking channel
	channel.input = make(chan T, 0)
	channel.output = make([]*outputChannel[T], 0)
	channel.context = ctx
	if logger != nil {
		channel.logger = logger
	}
	go channel.broadcast()
	return &channel
}

// WithReceiveChannelTimeout may be chained to have a seperate timeout for each message on each receipient.
func (ch *Broadcastchannel[T]) WithReceiveChannelTimeout(timeout time.Duration) *Broadcastchannel[T] {
	ch.receiveTimeout = &timeout
	return ch
}

// Publish publishes given value val to all subscribers
func (ch *Broadcastchannel[T]) Publish(val T) error {
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

// Subscribe subscribes a channel to the broadcast channel. An uuid ist returned to be able to unsubscribe from the broadcast channel und for debug log naming.
func (ch *Broadcastchannel[T]) Subscribe(subscriber chan T) (uuid.UUID, error) {
	err := ch.context.Err()
	if err != nil {
		return uuid.Nil, err
	}
	ch.outputLock.Lock()
	uid := uuid.New()
	ch.output = append(ch.output, &outputChannel[T]{channel: subscriber, id: uid})
	if ch.logger != nil {
		ch.logger.Debugf("New channel subscriber: %s", uid)
	}
	ch.outputLock.Unlock()
	return uid, nil
}

// Unsubscribe unsubscribes a channel from the broadcast channel. It will not receive further broadcasts. It will not close the channel.
func (ch *Broadcastchannel[T]) Unsubscribe(subscriptionId uuid.UUID) {
	if ch.logger != nil {
		ch.logger.Debugf("Remove channel subscriber: %s", subscriptionId.String())
	}
	ch.outputLock.Lock()
	newOutput := make([]*outputChannel[T], 0, len(ch.output)-1)
	for _, sub := range ch.output {
		if sub.id != subscriptionId {
			newOutput = append(newOutput, sub)
		}
	}
	ch.output = newOutput
	ch.outputLock.Unlock()
}

func (ch *Broadcastchannel[T]) broadcast() {
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

func (ch *Broadcastchannel[T]) send(channel *outputChannel[T], value T) {
	var ctx context.Context
	var indivCancelFunc func()
	if ch.receiveTimeout != nil {
		ictx, cancelFunc := context.WithTimeout(ch.context, *ch.receiveTimeout)
		ctx = ictx
		indivCancelFunc = cancelFunc
	} else {
		ctx = ch.context
	}
	select {
	case channel.channel <- value:
	case <-ctx.Done():
		if ch.logger != nil {
			ch.logger.Debugf("Could not deliver message to receiver %s: %s, channel blocking?", channel.id.String(), ctx.Err())
		}
		if indivCancelFunc != nil {
			indivCancelFunc()
		}
	}
}
