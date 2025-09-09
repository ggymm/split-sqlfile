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

const bufferSize = 16 * 1024 * 1024 // 16MB 缓冲区

var (
	dropReg   = regexp.MustCompile(`DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:` + "`" + `)?([^` + "`" + `\s]+)(?:` + "`" + `)?`)
	alterReg  = regexp.MustCompile(`ALTER\s+TABLE\s+(?:` + "`" + `)?([^` + "`" + `\s]+)(?:` + "`" + `)?`)
	createReg = regexp.MustCompile(`CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:` + "`" + `)?([^` + "`" + `\s]+)(?:` + "`" + `)?`)
	insertReg = regexp.MustCompile(`INSERT\s+INTO\s+(?:` + "`" + `)?([^` + "`" + `\s]+)(?:` + "`" + `)?`)
	updateReg = regexp.MustCompile(`UPDATE\s+(?:` + "`" + `)?([^` + "`" + `\s]+)(?:` + "`" + `)?`)
	deleteReg = regexp.MustCompile(`DELETE\s+FROM\s+(?:` + "`" + `)?([^` + "`" + `\s]+)(?:` + "`" + `)?`)
)

type Splitter struct {
	input  string // 输入文件路径
	output string // 输出目录路径

	count   int                 // 缓存总字节数，避免重复计算
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

func (s *Splitter) parseTable(stmt string) string {
	upper := strings.ToUpper(stmt)
	regexes := []*regexp.Regexp{
		insertReg, createReg, // 最常用
		updateReg, deleteReg, alterReg, dropReg, // 较少使用
	}
	for _, re := range regexes {
		if matches := re.FindStringSubmatch(upper); len(matches) > 1 {
			table := matches[1]
			if len(table) >= 2 && table[0] == '`' && table[len(table)-1] == '`' {
				table = table[1 : len(table)-1]
			}
			return strings.ToLower(table)
		}
	}
	return "misc"
}

func (s *Splitter) writeStatement(stmt, table string) error {
	_, exists := s.tables[table]
	if !exists {
		p := filepath.Join(s.output, table+".sql")
		f, err := os.Create(p)
		if err != nil {
			return fmt.Errorf("创建文件失败 %s: %w", p, err)
		}
		s.tables[table] = f
	}

	// 添加到缓冲区并更新计数器
	s.count += len(stmt)
	s.buffers[table] = append(s.buffers[table], stmt)

	// 使用缓存计数器，避免O(n²)复杂度
	if s.count > bufferSize {
		s.count = 0 // 重置计数器
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

		// 合并缓冲区
		str := &strings.Builder{}
		str.Grow(len(buffer) * 100)
		for _, buf := range buffer {
			str.WriteString(buf)
			str.WriteByte('\n')
		}

		// 写入目标文件
		file := s.tables[t]
		_, err := file.WriteString(str.String())
		if err != nil {
			return fmt.Errorf("写入文件失败: %w", err)
		}

		// 清空缓冲区释放内存
		s.buffers[t] = s.buffers[t][:0]
	}
	return nil
}

// Split 粗暴优化版：直接按分号分割处理
func (s *Splitter) Split() error {
	// 创建输出目录
	if err := os.MkdirAll(s.output, os.ModePerm); err != nil {
		return fmt.Errorf("创建输出目录失败: %w", err)
	}

	// 打开输入文件
	input, err := os.Open(s.input)
	if err != nil {
		return fmt.Errorf("打开输入文件失败: %w", err)
	}
	defer func() {
		_ = input.Close()
		s.closeFiles()
	}()

	// 获取文件大小并显示
	info, _ := os.Stat(s.input)
	fmt.Printf("正在处理文件: %s (%.2f GB)\n", s.input, float64(info.Size())/(1024*1024))

	// 分块读取并按分号分割
	var (
		n      int
		buffer = make([]byte, bufferSize)

		count int64
		start = time.Now()
		cache = &strings.Builder{}
	)

	for {
		n, err = input.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("读取文件失败: %w", err)
		}

		if n > 0 {
			cache.Write(buffer[:n])
			parts := strings.Split(cache.String(), ";")

			// 处理除最后一部分外的所有完整语句
			for i := 0; i < len(parts)-1; i++ {
				stmt := strings.TrimSpace(parts[i])
				if len(stmt) == 0 ||
					strings.HasPrefix(stmt, "/*") ||
					strings.HasPrefix(stmt, "--") {
					continue
				}

				// 解析表名并写入语句
				table := s.parseTable(stmt)
				err = s.writeStatement(stmt+";", table)
				if err != nil {
					return err
				}

				count++
				if count%5000 == 0 {
					elapsed := time.Since(start)
					fmt.Printf("\r已处理: %d 条SQL语句 - 用时: %v", count, elapsed.Round(time.Second))
				}
			}

			// 保留最后一个未完成的部分
			cache.Reset()
			if len(parts) > 0 {
				cache.WriteString(parts[len(parts)-1])
			}
		}
	}

	// 处理最后剩余的内容
	if cache.Len() > 0 {
		stmt := strings.TrimSpace(cache.String())
		if len(stmt) > 0 &&
			!strings.HasPrefix(stmt, "/*") &&
			!strings.HasPrefix(stmt, "--") {
			table := s.parseTable(stmt)
			if err = s.writeStatement(stmt, table); err != nil {
				return err
			}
			count++
		}
	}

	// 刷新所有缓冲区
	if err = s.flushBuffers(); err != nil {
		return err
	}

	// 显示最终结果
	elapsed := time.Since(start)
	fmt.Printf("\r处理完成！共处理 %d 条SQL语句 - 总用时: %v\n", count, elapsed.Round(time.Second))
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

	// 执行拆分操作
	splitter := NewSplitter(*input, *output)
	if err := splitter.Split(); err != nil {
		fmt.Printf("错误: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("文件拆分完成！")
}
