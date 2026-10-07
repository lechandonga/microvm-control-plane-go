// Command mvctl 是本地微虚拟机控制面的最小演示入口，全部使用
// 内置模拟后端运行，不依赖 KVM/QEMU/libvirt 或任何外部服务。
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	microvm "github.com/lechandonga/microvm-control-plane-go"
)

func main() {
	dir := filepath.Join(os.TempDir(), "mvctl-data")
	c, err := microvm.Open(microvm.Config{
		DataDir: dir,
		Hosts: []microvm.HostConfig{{
			Name: "node-local", CPUPhysical: 8, MemoryPhysicalMiB: 8192,
			StoragePhysicalMiB: 2048,
			CPUOvercommit:      2, MemoryOvercommit: 1.5, StorageOvercommit: 1,
		}},
	})
	must(err)
	defer c.Close()
	c.Backend().SetStepDelay(10 * time.Millisecond)

	ctx := context.Background()
	op, err := c.Define(microvm.VMSpec{
		ID: "vm-demo", Name: "demo", VCPU: 2, MemoryMiB: 1024, DiskMiB: 64,
	}, "idem-define-1")
	must(err)
	op, err = c.WaitOperation(ctx, op.ID)
	must(err)
	fmt.Printf("define -> %s\n", op.Status)

	for _, step := range []struct {
		name string
		run  func(string) (*microvm.Operation, error)
	}{
		{"start", func(k string) (*microvm.Operation, error) { return c.Start("vm-demo", k) }},
		{"stop", func(k string) (*microvm.Operation, error) { return c.Stop("vm-demo", k) }},
	} {
		op, err = step.run("idem-" + step.name + "-1")
		must(err)
		op, err = c.WaitOperation(ctx, op.ID)
		must(err)
		fmt.Printf("%s -> %s\n", step.name, op.Status)
	}

	vm, err := c.GetVM("vm-demo")
	must(err)
	usage, err := c.HostUsage(vm.Host)
	must(err)
	fmt.Printf("vm state=%s host=%s usage cpu=%d/%d mem=%d/%dMiB storage=%d/%dMiB\n",
		vm.State, vm.Host,
		usage.CPUUsed, usage.CPUCap, usage.MemoryUsedMiB, usage.MemoryCapMiB,
		usage.StorageUsedMiB, usage.StorageCapMiB)
	fmt.Printf("data dir: %s\n", dir)
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
