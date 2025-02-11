package main

import (
	"context"
	"github.com/pingcap/tidb/br/pkg/storage"
	"log"
	"math/rand"
	"sync"
	"time"
)

// Task 表示一个任务，使用 [begin, end) 表示任务需要生成的随机字符串数量
type Task struct {
	id       int
	begin    int
	end      int
	cols     []Column
	fileName string
}

// Result 表示生成结果，包含任务 id 以及生成的随机字符串集合
type Result struct {
	id       int
	fileName string
	values   [][]string
}

// generatorWorker 从 tasksCh 中获取任务，使用 sync.Pool 复用 []string 切片，生成随机字符串后发送到 resultsCh
func generatorWorker(tasksCh <-chan Task, resultsCh chan<- Result, workerID int, pool *sync.Pool, wg *sync.WaitGroup) {
	defer wg.Done()
	for task := range tasksCh {
		count := task.end - task.begin
		// 尝试从池中获取一个 [][]string 切片
		buf := pool.Get().([][]string)
		if cap(buf) < count {
			buf = make([][]string, count)
		}
		// 设定切片长度为 count
		values := buf[:count]

		log.Printf("Generator %d: 处理任务 %d, 范围 [%d, %d)，生成 %d 个随机字符串", workerID, task.id, task.begin, task.end, count)
		for i := 0; i < count; i++ {
			var row []string
			for _, col := range task.cols {
				row = append(row, generateValue(col))
			}
			values[i] = row
		}
		resultsCh <- Result{id: task.id, values: values, fileName: task.fileName}
	}
}

// writerWorker 从 resultsCh 中获取生成结果，并写入 CSV 文件后将使用完的切片放回 pool
func writerWorker(resultsCh <-chan Result, workerID int, pool *sync.Pool, wg *sync.WaitGroup) {
	defer wg.Done()
	op := storage.BackendOptions{GCS: storage.GCSBackendOptions{CredentialsFile: *credentialPath}}

	s, err := storage.ParseBackend("gcs://global-sort-dir", &op)
	if err != nil {
		panic(err)
	}
	store, err := storage.NewWithDefaultOpt(context.Background(), s)
	if err != nil {
		panic(err)
	}

	for result := range resultsCh {
		success := false
		fileName := result.fileName
		// 重试机制
		for attempt := 1; attempt <= maxRetries; attempt++ {
			if *localPath != "" {
				err = writeCSVToLocalDisk(*localPath+fileName, nil, result.values)
				if err != nil {
					log.Fatal("Error writing CSV:", err)
				}
			} else {
				err = writeDataToGCS(store, fileName, result.values)
			}
			if err == nil {
				log.Printf("Worker %d: 成功写入 %s (%d 行)", workerID, fileName, len(result.values))
				success = true
				break
			}

			log.Printf("Worker %d: 第 %d 次写入 GCS 失败: %v", workerID, attempt, err)

			// 指数退避策略：等待 `2^(attempt-1) * 100ms`（最大不超过 5s）
			waitTime := time.Duration(100*(1<<uint(attempt-1))) * time.Millisecond
			if waitTime > 4*time.Second {
				waitTime = 4 * time.Second
			}
			time.Sleep(waitTime + time.Duration(rand.Intn(500))*time.Millisecond) // 额外加一点随机时间，避免同时重试
		}
		if !success {
			log.Printf("Worker %d: 最终写入失败 %s (%d 行)", workerID, fileName, len(result.values))
		}

		// 将使用完的切片放回 pool 供后续复用
		pool.Put(result.values)
	}
}

//func main() {
//	// 解析命令行参数
//	flag.Parse()
//
//	// 列出 GCS 目录下的文件
//	if *showFile {
//		showFiles(*credentialPath)
//		return
//	}
//
//	// 删除指定文件
//	if *deleteFileName != "" {
//		deleteFile(*credentialPath, *deleteFileName)
//		return
//	}
//
//	// 读取指定文件前 1024 字节
//	if *glanceFile != "" {
//		glanceFiles(*credentialPath, *glanceFile)
//		return
//	}
//
//	log.Printf("配置参数: credential=%s, template=%s, concurrency=%d, rowCount=%d",
//		*credentialPath, *templatePath, *concurrency, *rowCount)
//
//	// 读取 SQL Schema
//	sqlSchema, err := readSQLFile(*templatePath)
//	if err != nil {
//		log.Fatalf("读取 SQL 模板失败: %v", err)
//	}
//
//	// 解析 Schema
//	columns := parseSQLSchema(sqlSchema)
//
//	if *rowCount <= 0 || *batchSize <= 0 {
//		log.Fatal("总数和每个批次的数量必须大于 0")
//	}
//
//	// 计算任务数量
//	taskCount := (*rowCount + *batchSize - 1) / *batchSize
//	log.Printf("总共将生成 %d 个任务，每个任务最多生成 %d 个随机字符串", taskCount, batchSize)
//
//	// 创建任务和结果的 channel
//	tasksCh := make(chan Task, taskCount)
//	resultsCh := make(chan Result, taskCount)
//
//	// 建立一个 sync.Pool 用于复用 []string 切片，初始容量为 batchSize
//	pool := &sync.Pool{
//		New: func() interface{} {
//			return make([][]string, *batchSize)
//		},
//	}
//
//	var wgGen sync.WaitGroup
//	// 启动 generator worker
//	for i := 0; i < *generatorNum; i++ {
//		wgGen.Add(1)
//		go generatorWorker(tasksCh, resultsCh, i, pool, &wgGen)
//	}
//
//	var wgWriter sync.WaitGroup
//	// 启动 writer worker
//	for i := 0; i < *writerNum; i++ {
//		wgWriter.Add(1)
//		go writerWorker(resultsCh, i, pool, &wgWriter)
//	}
//
//	// 将任务按照 [begin, end) 的范围进行分解，并发送到 tasksCh
//	taskID := 0
//	currentIndex := 0
//
//	for currentIndex < *rowCount {
//		begin := currentIndex
//		end := currentIndex + *batchSize
//		if end > *rowCount {
//			end = *rowCount
//		}
//		csvFileName := fmt.Sprintf("%s.%d.csv", *baseFileName, taskID)
//		task := Task{
//			id:       taskID,
//			begin:    begin,
//			end:      end,
//			cols:     columns,
//			fileName: csvFileName,
//		}
//		tasksCh <- task
//		taskID++
//		currentIndex = end
//	}
//	close(tasksCh) // 任务分发完毕后关闭 tasksCh
//
//	// 等待所有 generator 完成后关闭 resultsCh
//	wgGen.Wait()
//	close(resultsCh)
//
//	// 等待所有 writer 完成写入
//	wgWriter.Wait()
//
//	log.Printf("Done！")
//}
