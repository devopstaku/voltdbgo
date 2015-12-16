package main

import (
	"fmt"
	"log"
	"sort"

	"github.com/devopstaku/voltdbgo/voltdb"
)

// StatsProcedure is the result of "@Statistics PROCEDURE"
type StatsProcedure struct {
	Timestamp        int64
	HostId           int64
	Hostname         string
	SiteId           int64
	PartitionId      int64
	Procedure        string
	Invocations      int64
	TimedInvocations int64
	MinExecTime      int64
	MaxExecTime      int64
	AvgExecTime      int64
	MinResultSize    int64
	MaxResultSize    int64
	AvgResultSize    int64
	MinParamSetSize  int64
	MaxParamSetSize  int64
	AvgParamSetSize  int64
	Aborts           int64
	Failures         int64
}

func (p StatsProcedure) weight() int64 {
	return p.AvgExecTime * p.TimedInvocations
}

// ProcedureCost summarizes average execution by procedure
type ProcedureCost struct {
	Procedure        string
	Invocations      int64
	TimedInvocations int64
	AvgExecTime      int64
}

func (p ProcedureCost) String() string {
	return fmt.Sprintf("Procedure:%v, Invocations:%v, Timed:%v, AvgExecution:%v, Cost:%#v",
		p.Procedure, p.Invocations, p.TimedInvocations, p.AvgExecTime, p.weight())
}

func (p ProcedureCost) weight() int64 {
	return p.AvgExecTime * p.TimedInvocations
}

// ProcedureCost allows sorting ProcedureCost by weighted execution time.
type ProcedureCostList []ProcedureCost

func (p ProcedureCostList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p ProcedureCostList) Len() int           { return len(p) }
func (p ProcedureCostList) Less(i, j int) bool { return p[i].weight() > p[j].weight() }

func sortProcedureCostMap(m map[string]ProcedureCost) ProcedureCostList {
	sortedStats := make(ProcedureCostList, len(m))
	i := 0
	for _, v := range m {
		sortedStats[i] = v
		i++
	}
	sort.Sort(sortedStats)
	return sortedStats
}

// main. Cleaning lady!?
func main() {
	volt := connectOrDie()
	defer volt.Close()
	dumpProcedureCost(volt)
}

func connectOrDie() *voltdb.Conn {
	volt, err := voltdb.NewConnection("username", "", "localhost:21212")
	if err != nil {
		log.Fatalf("Connection error %v\n", err)
	}
	if !volt.TestConnection() {
		log.Fatalf("Connection error: failed to ping VoltDB database.")
	}
	return volt
}

// dumpProcedureCost prints procedures ordered by (Invocations * AvgExecTime)
func dumpProcedureCost(volt *voltdb.Conn) {
	response, err := volt.Call("@Statistics", "PROCEDURE", 0)
	if err != nil {
		log.Fatalf("Error calling @Statistics PROCEDURE %v\n", err)
	}

	// accumulate running totals in this map by procedure name
	statsByProcedure := make(map[string]ProcedureCost)
	table := response.Table(0)
	for table.HasNext() {
		var row StatsProcedure
		if err := table.Next(&row); err != nil {
			log.Fatalf("Table iteration error %v\n", err)
		}
		if exists, ok := statsByProcedure[row.Procedure]; ok == true {
			// weighted average of exec times by timedInvocations
			avg := (exists.weight() + row.weight()) / (exists.TimedInvocations + row.TimedInvocations)
			exists.Invocations += row.Invocations
			exists.TimedInvocations += row.TimedInvocations
			exists.AvgExecTime = avg
		} else {
			var newEntry ProcedureCost
			newEntry.Procedure = row.Procedure
			newEntry.Invocations = row.Invocations
			newEntry.TimedInvocations = row.TimedInvocations
			newEntry.AvgExecTime = row.AvgExecTime
			statsByProcedure[newEntry.Procedure] = newEntry
		}
	}

	sorted := sortProcedureCostMap(statsByProcedure)
	var ttlWeight int64 = 0
	for _, stat := range sorted {
		ttlWeight += stat.weight()
	}
	for _, stat := range sorted {
		fmt.Printf("%0.1f%% %v\n", float64(stat.weight())/float64(ttlWeight)*100, stat)
	}
}
