package system

import (
	"github.com/siddontang/go-log/log"
	"github.com/spf13/cobra"
	"github.com/mymmsc/mysql-replicator/src/helpers"
	"github.com/mymmsc/mysql-replicator/src/models/master"
	"math/rand"
	"strconv"
	"time"
)

const (
	// goroutine count. WARNING if you set more 1, may be concurrency problems
	ThreadCount = 1
	// time to create queries in minutes
	LoadTime = 60
)

var CmdLoad = &cobra.Command{
	Use:   "load",
	Short: "Create queries to master",
	Long:  "Create queries to master",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		load()
	},
}

var counters map[int]int

func load() {
	log.Info("Start loader")
	helpers.MakeCredentials()
	counters = make(map[int]int)

	for i := 0; i < ThreadCount; i++ {
		log.Infof("Create goroutine #%s", strconv.Itoa(i+1))
		counters[i] = 0
		go makeQueries(i)
	}

	time.Sleep(LoadTime * time.Minute)
	showStats()
	log.Info("Stop loader")
}

func showStats() {
	totalQueries := 0

	for i := 0; i < ThreadCount; i++ {
		log.Infof("Goroutine create %s queries per %s minute(s)", strconv.Itoa(counters[i]), strconv.Itoa(LoadTime))
		totalQueries = totalQueries + counters[i]
	}

	queriesPerMinute := totalQueries / LoadTime
	log.Infof("Total queries: %s; Queries per minute: %s", strconv.Itoa(totalQueries), strconv.Itoa(queriesPerMinute))
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

func makeQueries(id int) {
	queries := []string{
		"INSERT INTO test.user (`name`, `status`, `active`, `balance`, `time`, `date`, `datetime`) VALUE ('Jack', 'active', false, 5.56, '08:00:50', '2001-03-10', '2001-03-10 17:16:18');",
		"UPDATE test.user SET `name`='Tommy', status='dead', active=true, balance=7.62, time='06:00:58', date='2010-03-10', datetime='2010-03-10 17:16:10' ORDER BY RAND() LIMIT 1",
		"DELETE FROM test.user ORDER BY RAND() LIMIT 1;",
		"INSERT INTO test.post (`title`, `text`) VALUE ('Title', 'London is the capital of Great Britain');",
		"UPDATE test.post SET title='New title' ORDER BY RAND() LIMIT 1;",
		"DELETE FROM test.post ORDER BY RAND() LIMIT 1;",
		"INSERT INTO test.news (`title`, `text`) VALUE ('Title', 'London is the capital of Great Britain');",
		"UPDATE test.news SET title='New title' ORDER BY RAND() LIMIT 1;",
		"DELETE FROM test.news ORDER BY RAND() LIMIT 1;",
		"INSERT INTO test.log (`event`) VALUES ('bang!');",
		"INSERT INTO test.log (`event`) VALUES ('bong!');",
		"UPDATE test.log SET event='hei';",
		"INSERT INTO test.log (`event`) VALUES ('aaa'), ('qqq'), ('www'), ('eee'), ('rrr');",
	}

	rand.Seed(time.Now().UTC().UnixNano())

	var query string
	var result bool

	counter := 0
	for {
		query = queries[randInt(0, len(queries))]

		result = master.Exec(helpers.Query{
			Query:  query,
			Params: []interface{}{},
		})

		if result == true {
			counter++
			counters[id] = counter
		}
	}
}
