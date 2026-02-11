package chanqueue_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gammazero/chanqueue"
	"go.uber.org/goleak"
)

func TestCapLen(t *testing.T) {
	defer goleak.VerifyNone(t)

	cq := chanqueue.New[int]()
	if cq.Cap() != -1 {
		t.Error("expected capacity -1")
	}
	cq.Close()

	cq = chanqueue.New[int](chanqueue.WithCapacity[int](3))
	if cq.Cap() != 3 {
		t.Error("expected capacity 3")
	}
	if cq.Len() != 0 {
		t.Error("expected 0 from Len()")
	}
	in := cq.In()
	for i := 0; i < cq.Cap(); i++ {
		if cq.Len() != i {
			t.Errorf("expected %d from Len()", i)
		}
		in <- i
	}
	cq.Shutdown()

	cq = chanqueue.New(chanqueue.WithCapacity[int](0))
	if cq.Cap() != -1 {
		t.Error("expected capacity -1")
	}
	cq.Close()
}

func TestExistingInput(t *testing.T) {
	defer goleak.VerifyNone(t)

	in := make(chan int, 1)
	cq := chanqueue.New(chanqueue.WithInput[int](in), chanqueue.WithCapacity[int](64))
	in <- 42
	x := <-cq.Out()
	if x != 42 {
		t.Fatal("wrong value")
	}
	cq.Close()
}

func TestExistingRdOnlyInput(t *testing.T) {
	defer goleak.VerifyNone(t)

	in := make(chan int, 1)
	var inRdOnly <-chan int = in
	cq := chanqueue.New(chanqueue.WithInputRdOnly[int](inRdOnly), chanqueue.WithCapacity[int](64))
	in <- 42
	x := <-cq.Out()
	if x != 42 {
		t.Fatal("wrong value")
	}

	cq.Close()
	select {
	case <-cq.Out():
		t.Fatal("out channel shound not be closed yet")
	case <-time.After(time.Millisecond):
	}

	close(in)
	select {
	case <-cq.Out():
	case <-time.After(time.Millisecond):
		t.Fatal("timed out waiting for output channel to close")
	}
}

func TestExistingOutput(t *testing.T) {
	defer goleak.VerifyNone(t)

	out := make(chan int)
	cq := chanqueue.New(chanqueue.WithOutput[int](out))
	cq.In() <- 42
	x := <-out
	if x != 42 {
		t.Fatal("wrong value")
	}
	cq.Close()
}

func TestExistingChannels(t *testing.T) {
	defer goleak.VerifyNone(t)

	in := make(chan int)
	out := make(chan int)

	// Create a buffer between in and out channels.
	chanqueue.New(chanqueue.WithInput[int](in), chanqueue.WithOutput[int](out))
	for i := 0; i <= 100; i++ {
		in <- i
	}
	close(in) // this will close ch when all output is read.

	expect := 0
	for x := range out {
		if x != expect {
			t.Fatalf("expected %d got %d", expect, x)
		}
		expect++
	}
}

func TestUnlimitedSpace(t *testing.T) {
	defer goleak.VerifyNone(t)

	const msgCount = 1000
	cq := chanqueue.New[int]()
	go func() {
		for i := range msgCount {
			cq.In() <- i
		}
		cq.Close()
	}()
	for i := range msgCount {
		val := <-cq.Out()
		if i != val {
			t.Fatal("expected", i, "but got", val)
		}
	}
}

func TestLimitedSpace(t *testing.T) {
	defer goleak.VerifyNone(t)

	const msgCount = 1000
	cq := chanqueue.New(chanqueue.WithCapacity[int](32))
	go func() {
		for i := range msgCount {
			cq.In() <- i
		}
		cq.Close()
	}()
	for i := range msgCount {
		val := <-cq.Out()
		if i != val {
			t.Fatal("expected", i, "but got", val)
		}
	}
}

func TestBufferLimit(t *testing.T) {
	defer goleak.VerifyNone(t)

	cq := chanqueue.New(chanqueue.WithCapacity[int](32), chanqueue.WithBaseCapacity[int](1000))
	defer cq.Shutdown()

	for i := 0; i < cq.Cap(); i++ {
		cq.In() <- i
	}
	select {
	case cq.In() <- 999:
		t.Fatal("expected timeout on full channel")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestRace(t *testing.T) {
	defer goleak.VerifyNone(t)

	cq := chanqueue.New[int]()
	defer cq.Shutdown()

	var err error
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			if cq.Len() > 1000 {
				err = errors.New("Len too great")
			}
			if cq.Cap() != -1 {
				err = errors.New("expected Cap to return -1")
			}
		}
	}()

	ready := make(chan struct{}, 2)
	start := make(chan struct{})
	go func() {
		ready <- struct{}{}
		<-start
		for i := range 1000 {
			cq.In() <- i
		}
	}()

	var val int
	go func() {
		ready <- struct{}{}
		<-start
		for range 1000 {
			val = <-cq.Out()
		}
		close(done)
	}()

	<-ready
	<-ready
	close(start)
	<-done
	if val != 999 {
		t.Fatalf("last value should be 999, got %d", val)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func TestDouble(t *testing.T) {
	defer goleak.VerifyNone(t)

	const msgCount = 1000
	cq := chanqueue.New(chanqueue.WithCapacity[int](100))
	recvCq := chanqueue.New(chanqueue.WithCapacity[int](100))
	go func() {
		for i := range msgCount {
			cq.In() <- i
		}
		cq.Close()
	}()
	var err error
	go func() {
		var i int
		for val := range cq.Out() {
			if i != val {
				err = fmt.Errorf("expected %d but got %d", i, val)
				return
			}
			recvCq.In() <- i
			i++
		}
		if i != msgCount {
			err = fmt.Errorf("expected %d messages from ch, got %d", msgCount, i)
			return
		}
		recvCq.Close()
	}()
	var i int
	for val := range recvCq.Out() {
		if i != val {
			t.Fatal("expected", i, "but got", val)
		}
		i++
	}
	if err != nil {
		t.Fatal(err)
	}
	if i != msgCount {
		t.Fatalf("expected %d messages from recvCh, got %d", msgCount, i)
	}
}

func TestDeadlock(t *testing.T) {
	defer goleak.VerifyNone(t)

	cq := chanqueue.New(chanqueue.WithCapacity[int](1))
	defer cq.Shutdown()
	cq.In() <- 1
	<-cq.Out()

	done := make(chan struct{})
	go func() {
		cq.In() <- 2
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Millisecond):
		t.Fatal("could not write to channel")
	}
}

func TestStop(t *testing.T) {
	defer goleak.VerifyNone(t)

	var cq *chanqueue.ChanQueue[int]
	testStop := func(t *testing.T) {
		for i := range 10 {
			cq.In() <- i
		}

		if !cq.Stop() {
			t.Fatal("expected stopped to be true")
		}
		if cq.Stop() {
			t.Fatal("expected 2nd stop to be false")
		}

		select {
		case cq.In() <- 11:
			t.Fatal("queue should not accept input")
		default:
		}

		for range cq.Out() {
			t.Fatal("expected no output from stopped queue")
		}
	}

	cq = chanqueue.New[int]()
	t.Run("queue", testStop)

	cq = chanqueue.NewRing(chanqueue.WithCapacity[int](5))
	t.Run("ring", testStop)

	cq = chanqueue.NewRing(chanqueue.WithCapacity[int](1))
	t.Run("one-ring", testStop)
}

func TestRing(t *testing.T) {
	defer goleak.VerifyNone(t)

	cq := chanqueue.NewRing(chanqueue.WithCapacity[rune](5))
	for _, r := range "hello" {
		cq.In() <- r
	}

	cq.In() <- 'w'
	char := <-cq.Out()
	if char != 'e' {
		t.Fatal("expected 'e' but got", char)
	}

	for _, r := range "abcdefghij" {
		cq.In() <- r
	}

	cq.Close()

	out := make([]rune, 0, cq.Len())
	for r := range cq.Out() {
		out = append(out, r)
	}
	if string(out) != "fghij" {
		t.Fatalf("expected \"fghij\" but got %q", out)
	}
}

func TestRingNoLimit(t *testing.T) {
	inCh := make(chan rune)
	outCh := make(chan rune)
	// Test that options are passed through to New.
	cq := chanqueue.NewRing(chanqueue.WithCapacity[rune](0),
		chanqueue.WithInput[rune](inCh), chanqueue.WithOutput[rune](outCh))
	if cq.Cap() != -1 {
		t.Fatal("expected -1 capacity")
	}
	inCh <- 'A'
	cq.Close()

	var count int
	to := time.After(time.Second)

loop:
	for {
		select {
		case char, open := <-outCh:
			if !open {
				break loop
			}
			count++
			if char != 'A' {
				t.Fatal("wrong character returned:", char)
			}
		case <-to:
			t.Fatal("timed out waiting for out channel to close")
		}
	}

	if count != 1 {
		t.Fatal("wrong number of characters returned:", count)
	}
}

func TestOneRing(t *testing.T) {
	defer goleak.VerifyNone(t)

	cq := chanqueue.NewRing(chanqueue.WithCapacity[rune](1))
	for _, r := range "hello" {
		cq.In() <- r
	}

	cq.In() <- 'w'
	if cq.Len() != 1 {
		t.Fatalf("expected length 1, got %d", cq.Len())
	}
	char := <-cq.Out()
	if char != 'w' {
		t.Fatal("expected 'w' but got", char)
	}
	if cq.Len() != 0 {
		t.Fatal("expected length 0")
	}

	for _, r := range "abcdefghij" {
		cq.In() <- r
	}

	cq.Close()

	out := make([]rune, 0, cq.Len())
	for r := range cq.Out() {
		out = append(out, r)
	}
	if string(out) != "j" {
		t.Fatalf("expected \"j\" but got %q", out)
	}

	cq = chanqueue.NewRing[rune]()
	if cq.Cap() != -1 {
		t.Fatal("expected -1 capacity")
	}
	cq.Close()
}

func TestCloseMultiple(t *testing.T) {
	cq := chanqueue.New[string]()
	cq.Close()
	cq.Close()
	cq.Shutdown()
	cq.Shutdown()
}

func TestReadmeExample(t *testing.T) {
	defer goleak.VerifyNone(t)

	cq := chanqueue.New[int]()
	result := make(chan int)

	// Start consumer.
	go func(r <-chan int) {
		var sum int
		for i := range r {
			sum += i
		}
		result <- sum
	}(cq.Out())

	// Write numbers to queue.
	for i := 1; i <= 10; i++ {
		cq.In() <- i
	}
	cq.Close()

	// Wait for consumer to send result.
	val := <-result
	fmt.Println("Result:", val)

	if val != 55 {
		t.Fatal("expected differrent result")
	}
}

func BenchmarkSerial(b *testing.B) {
	cq := chanqueue.New[int]()
	for i := 0; i < b.N; i++ {
		cq.In() <- i
	}
	for i := 0; i < b.N; i++ {
		<-cq.Out()
	}
}

func BenchmarkParallel(b *testing.B) {
	cq := chanqueue.New[int]()
	go func() {
		for i := 0; i < b.N; i++ {
			<-cq.Out()
		}
		<-cq.Out()
	}()
	for i := 0; i < b.N; i++ {
		cq.In() <- i
	}
	cq.Close()
}

func BenchmarkPushPull(b *testing.B) {
	cq := chanqueue.New[int]()
	for i := 0; i < b.N; i++ {
		cq.In() <- i
		<-cq.Out()
	}
}
