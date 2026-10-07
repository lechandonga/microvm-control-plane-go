package microvm

import "sort"

func sortVMs(vms []VM) {
	sort.Slice(vms, func(i, j int) bool { return vms[i].CreatedAt.Before(vms[j].CreatedAt) })
}
