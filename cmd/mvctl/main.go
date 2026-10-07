// Command mvctl runs a self-contained demo of the micro-VM control plane
// against the in-process simulated backend. Everything lives on local disk;
// no hypervisor, network service or credentials are involved.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/lechandonga/microvm-control-plane-go/pkg/microvm"
)

func main() {
	dir := flag.String("dir", "./.mvctl-data", "durable data directory")
	flag.Parse()

	cp, err := microvm.Open(microvm.Config{
		Dir: *dir,
		Hosts: []microvm.HostConfig{
			{ID: "host-a", CPUCapacity: 8, MemCapacity: 8192, DiskCapacity: 100_000},
		},
		Overcommit:   microvm.OvercommitRatio{CPU: 2, Mem: 1.5},
		BackendDelay: 2 * time.Millisecond,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	defer cp.Close()

	ctx := context.Background()
	run := func(name string, opID string, err error) {
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s submit failed: %v\n", name, err)
			os.Exit(1)
		}
		if op, err := cp.Wait(ctx, opID); err != nil {
			fmt.Fprintf(os.Stderr, "%s -> %s: %v\n", name, op.State, err)
			os.Exit(1)
		} else {
			fmt.Printf("%s -> %s\n", name, op.State)
		}
	}

	id, e := cp.Define(microvm.Spec{ID: "demo-vm", Name: "demo", VCPU: 2, Memory: 1024, Disk: 4096}, "demo-define")
	run("define", id, e)
	id, e = cp.Start("demo-vm", "demo-start")
	run("start", id, e)
	id, e = cp.WriteBlock("demo-vm", 0, []byte("hello world"), "demo-write")
	run("write", id, e)
	id, e = cp.TakeSnapshot("demo-vm", "snap-1", "demo-snap")
	run("snapshot", id, e)
	id, e = cp.Stop("demo-vm", "demo-stop")
	run("stop", id, e)
	id, e = cp.Destroy("demo-vm", "demo-destroy")
	run("destroy", id, e)

	for _, u := range cp.HostUsages() {
		fmt.Printf("host %s ledger: cpu %d/%d mem %d/%d disk %d/%d\n",
			u.ID, u.CPUUsed, u.CPUCapacity, u.MemUsed, u.MemCapacity, u.DiskUsed, u.DiskCapacity)
	}
}
