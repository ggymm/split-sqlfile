// SQL文件拆分工具 - 按表名将大型SQL文件拆分为多个小文件
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

const bufSize = 64 * 1024 // 64KB 缓冲区

type Splitter struct {
	input  string // 输入文件路径
	output string // 输出目录路径

	tables  map[string]*os.File // 按表名缓存的文件句柄
	buffers map[string][]string // 按表名缓存的SQL语句缓冲区
}

func NewSplitter(inputFile, outputDir string) *Splitter {
	return &Splitter{
		input:   inputFile,
		output:  outputDir,
		tables:  make(map[string]*os.File),
		buffers: make(map[string][]string),
	}
}

func (s *Splitter) hasValid(statement string) bool {
	hasValid := false
	if statement != "" {
		for _, line := range strings.Split(statement, "\n") {
			trimmed := strings.TrimSpace(line)
			if trimmed != "" && !strings.HasPrefix(trimmed, "--") && !strings.HasPrefix(trimmed, "/*") {
				hasValid = true
				break
			}
		}
	}
	return hasValid
}

func (s *Splitter) extractTable(statement string) string {
	upper := strings.ToUpper(strings.TrimSpace(statement))

	// SQL语句模式：支持CREATE TABLE、INSERT、UPDATE、DELETE、ALTER TABLE、DROP TABLE
	patterns := []string{
		`CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:` + "`" + `)?([^` + "`" + `\s]+)(?:` + "`" + `)?`,
		`INSERT\s+INTO\s+(?:` + "`" + `)?([^` + "`" + `\s]+)(?:` + "`" + `)?`,
		`UPDATE\s+(?:` + "`" + `)?([^` + "`" + `\s]+)(?:` + "`" + `)?`,
		`DELETE\s+FROM\s+(?:` + "`" + `)?([^` + "`" + `\s]+)(?:` + "`" + `)?`,
		`ALTER\s+TABLE\s+(?:` + "`" + `)?([^` + "`" + `\s]+)(?:` + "`" + `)?`,
		`DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:` + "`" + `)?([^` + "`" + `\s]+)(?:` + "`" + `)?`,
	}

	for _, pattern := range patterns {
		re := regexp.MustCompile(pattern)
		if matches := re.FindStringSubmatch(upper); len(matches) > 1 {
			// 转为小写并去除反引号
			return strings.Trim(strings.ToLower(matches[1]), "`")
		}
	}
	return "" // 未识别的语句
}

// writeStatement 将SQL语句写入对应表名的文件（合并了缓冲和文件操作）
func (s *Splitter) writeStatement(table, statement string) error {
	if table == "" {
		table = "misc" // 无法识别表名的语句归类为misc
	}

	// 获取或创建文件句柄
	_, exists := s.tables[table]
	if !exists {
		p := filepath.Join(s.output, table+".sql")
		f, err := os.Create(p)
		if err != nil {
			return fmt.Errorf("创建文件失败 %s: %w", p, err)
		}
		s.tables[table] = f
	}

	// 添加到缓冲区
	s.buffers[table] = append(s.buffers[table], statement)

	// 检查是否需要刷新缓冲区（每1000条语句或缓冲区过大时）
	total := 0
	for _, buf := range s.buffers {
		total += len(buf)
	}
	if total > 1000 {
		return s.flushBuffers()
	}
	return nil
}

func (s *Splitter) closeFiles() {
	for _, file := range s.tables {
		_ = file.Close()
	}
}

func (s *Splitter) flushBuffers() error {
	for t, buffer := range s.buffers {
		if len(buffer) == 0 {
			continue
		}

		file := s.tables[t]
		for _, buf := range buffer {
			if _, err := file.WriteString(buf + "\n"); err != nil {
				return fmt.Errorf("写入文件失败: %w", err)
			}
		}

		// 清空缓冲区释放内存
		s.buffers[t] = s.buffers[t][:0]
	}

	return nil
}

// Split 执行SQL文件拆分的主要方法（使用缓冲区读取优化）
func (s *Splitter) Split() error {
	var (
		err error

		input     *os.File
		inputInfo os.FileInfo

		startTime      = time.Now()
		totalBytes     = inputInfo.Size()
		processedBytes = int64(0)

		leftover       string // 存储缓冲区边界上的不完整语句
		statementCount int64
	)

	// 打开输入文件
	input, err = os.Open(s.input)
	if err != nil {
		return fmt.Errorf("打开输入文件失败: %w", err)
	}
	defer func() {
		_ = input.Close()
		s.closeFiles()
	}()

	// 获取输入文件信息
	inputInfo, err = os.Stat(s.input)
	if err != nil {
		return fmt.Errorf("获取输入文件信息失败: %w", err)
	}

	// 创建输出目录
	err = os.MkdirAll(s.output, os.ModePerm)
	if err != nil {
		return fmt.Errorf("创建输出目录失败: %w", err)
	}

	fmt.Printf("正在处理文件: %s (%.2f GB)\n", s.input, float64(totalBytes)/(1024*1024*1024))

	n := 0
	buffer := make([]byte, bufSize)
	for {
		n, err = input.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("读取文件失败: %w", err)
		}
		if n > 0 {
			processedBytes += int64(n)

			// 内联进度显示（每处理10MB显示一次）
			if processedBytes%(10*1024*1024) == 0 && totalBytes > 0 {
				percentage := float64(processedBytes) / float64(totalBytes) * 100
				elapsed := time.Since(startTime)
				estimatedTotal := time.Duration(float64(elapsed) * float64(totalBytes) / float64(processedBytes))
				remaining := estimatedTotal - elapsed
				fmt.Printf("\r进度: %.2f%% (%d/%d 字节) - 已用时: %v - 预计剩余: %v",
					percentage, processedBytes, totalBytes,
					elapsed.Round(time.Second), remaining.Round(time.Second))
			}

			// 将读取的数据与上次的剩余数据合并
			chunk := leftover + string(buffer[:n])
			leftover = "" // 清空剩余数据

			// 按分号分割语句
			statements := strings.Split(chunk, ";")

			// 处理除最后一个外的所有语句（它们都是完整的）
			for i := 0; i < len(statements)-1; i++ {
				statement := strings.TrimSpace(statements[i])
				if s.hasValid(statement) {
					err = s.writeStatement(s.extractTable(statement), statement+";")
					if err != nil {
						return err
					}
					statementCount++
				}
			}

			// 最后一个部分可能是不完整的语句
			lastPart := strings.TrimSpace(statements[len(statements)-1])
			if err == io.EOF {
				// 文件结束，处理最后一个语句（如果有的话）
				if s.hasValid(lastPart) {
					err = s.writeStatement(s.extractTable(lastPart), lastPart)
					if err != nil {
						return err
					}
					statementCount++
				}
			} else {
				// 不是文件结束，保存为下次处理的剩余数据
				leftover = lastPart
			}
		}
	}

	// 最终刷新所有缓冲区
	err = s.flushBuffers()
	if err != nil {
		return err
	}

	// 显示最终进度和结果
	if totalBytes > 0 {
		percentage := float64(processedBytes) / float64(totalBytes) * 100
		elapsed := time.Since(startTime)
		fmt.Printf("\r进度: %.2f%% (%d/%d 字节) - 总用时: %v",
			percentage, processedBytes, totalBytes, elapsed.Round(time.Second))
	}
	fmt.Printf("\n处理完成！共处理 %d 条SQL语句\n", statementCount)
	return nil
}

func main() {
	help := flag.Bool("help", false, "显示帮助信息")
	input := flag.String("input", "", "输入SQL文件路径")
	output := flag.String("output", "output", "输出目录路径")
	flag.Parse()

	// 显示帮助信息
	if *help {
		fmt.Println("SQL文件拆分工具 - 按表名拆分大型SQL文件")
		fmt.Println("使用方法:")
		fmt.Println("  -help    显示帮助信息")
		fmt.Println("  -input   输入文件路径 (必需)")
		fmt.Println("  -output  输出目录路径 (必需)")
		fmt.Println()
		fmt.Println("示例:")
		fmt.Println("  split-sqlfile -input database.sql -output split_files")
		return
	}

	// 验证输入参数
	if *input == "" {
		fmt.Println("错误: 需要指定输入文件。使用 -help 查看使用说明。")
		os.Exit(1)
	}
	// 验证输出参数
	if *output == "" {
		fmt.Println("错误: 需要指定输出目录。使用 -help 查看使用说明。")
		os.Exit(1)
	}
	fmt.Printf("拆分文件: %s\n", *input)
	fmt.Printf("输出目录: %s\n", *output)

	// 创建拆分器
	splitter := NewSplitter(*input, *output)

	// 执行拆分操作
	if err := splitter.Split(); err != nil {
		fmt.Printf("错误: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("文件拆分完成！")
}
