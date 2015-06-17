package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	//"math"
	"math/rand"
	"os"
	"strconv"
	"time"
)

type Opts struct {
	DdlFile  string
	OutFile  string
	Order    string
	OrderInt int
}

// TODO PUT THIS IN IT'S OWN LOCATION!
type Report struct {
	Name       string
	SqlFile    string
	Parameters []Parameter
	Fields     []Field
}

// define sorting Reports by name
type ReportByName []Report

func (r ReportByName) Len() int           { return len(r) }
func (r ReportByName) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r ReportByName) Less(i, j int) bool { return r[i].Name < r[j].Name }

type Parameter struct {
	Name   string
	Type   string
	DBType string
}

type Field struct {
	ColName  string
	Type     string
	Nullable string
}

// -- //

func main() {
	var (
		ddlfile = flag.String("ddl", "", "ddl file")
		outfile = flag.String("out", "generated_data.json", "out file")
		order   = flag.String("order", "1", "Order: 10^x number of rows to generate")
	)

	flag.Parse()
	opts := &Opts{DdlFile: *ddlfile, OutFile: *outfile, Order: *order}
	if !validate(opts) {
		flag.Usage()
		os.Exit(1)
	}

	reports := make([]Report, 0, 20)

	// open the ddl file
	infile, err := os.Open(opts.DdlFile)
	if err != nil {
		log.Fatalf("Trouble opening file:%s. %s", opts.DdlFile, err)
	}
	defer infile.Close()

	// parse the ddl file
	decoder := json.NewDecoder(infile)
	for {
		var rep Report
		if err = decoder.Decode(&rep); err == io.EOF {
			break
		} else if err != nil {
			switch e := err.(type) {
			default:
				log.Fatalf("Trouble decoding json:%s", e)
			case *json.SyntaxError:
				log.Fatalf("Syntax error at offset:%d %s", e.Offset, e)
			}
		}
		reports = append(reports, rep)
	}

	ofile, err := os.Create(opts.OutFile)
	if err != nil {
		log.Fatalf("Trouble opening output file:%s", err)
	}
	defer ofile.Close()

	for i := range reports {
		processReport(&reports[i], opts.OrderInt, ofile)
	}
}

// Generator type for generating values
type Generator func() interface{}

// noop generator
func NoOpGenerator() interface{} {
	return ""
}

// Generator for Ints
func IntGenerator() interface{} {
	return rand.Int31()
}

// Generator for longs
func LongGenerator() interface{} {
	return rand.Int63()
}

// Generator for decimal
func DecimalGenerator() interface{} {
	return rand.Int63() / 100
}

// Generator for strings
type StringGenerator struct{}

const (
	ALPHABET    = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	ALPHABETLEN = len(ALPHABET)
)

func (sg *StringGenerator) Generate() interface{} {
	length := int32(rand.Int31n(10) + 10 + 1)
	b := &bytes.Buffer{}
	var i int32
	for i = 0; i < length; i++ {
		b.WriteByte(ALPHABET[rand.Int31n(int32(ALPHABETLEN))])
	}
	return b.String()
}

// generator for dates
type DateTimeGenerator struct {
	MinimumOcurrance int
}

// Generate type
func (dg *DateTimeGenerator) Generate() interface{} {
	r := rand.Int63n(10000000000)
	// approximately 1 out of every MinimumOcurrance will be datetime minimum
	if dg.MinimumOcurrance != 0 && r%int64(dg.MinimumOcurrance) == 0 {
		return "0001-01-01T00:00:00"
	}
	t := time.Unix(r, 0)
	// Mon Jan 2 15:04:05 -0700 MST 2006
	return t.Format("2006-01-02T15:04:05")
}

// generator for bools
func BoolGenerator() interface{} {
	return rand.Int31()%2 == 0
}

func processReport(rep *Report, order int, ofile io.Writer) {
	log.Printf("Report Name:%s", rep.Name)

	// generator map
	gm := make(map[string]Generator)
	// assign a generator for each field
	for i := 0; i < len(rep.Fields); i++ {
		field := rep.Fields[i]
		switch field.Type {
		case "string":
			gm[field.ColName] = (&StringGenerator{}).Generate
		case "int":
			gm[field.ColName] = IntGenerator
		case "long":
			gm[field.ColName] = LongGenerator
		case "decimal":
			gm[field.ColName] = DecimalGenerator
		case "datetime":
			gm[field.ColName] = (&DateTimeGenerator{MinimumOcurrance: 100}).Generate
		case "bool":
			gm[field.ColName] = BoolGenerator
		default:
			log.Printf("Warning: no operator for type:%s", field.Type)
			gm[field.ColName] = NoOpGenerator
		}
	}

	ofile.Write([]byte("{\n"))
	ofile.Write([]byte(fmt.Sprintf("\"%ss\":[\n", rep.Name)))
	// generate x datarows
	for i := 0; i < order; i++ {
		// data row
		dr := make(map[string]interface{})
		// for each field call the generator if it exists
		for j := 0; j < len(rep.Fields); j++ {
			cn := rep.Fields[j].ColName
			g, ok := gm[cn]
			if !ok {
				continue
			}
			dr[cn] = g()
		}

		// write the data row
		rowstr, err := json.Marshal(dr)
		if err != nil {
			log.Print(err)
			continue
		}
		ofile.Write(rowstr)
		ofile.Write([]byte("\n"))
	}
	ofile.Write([]byte("]\n}\n"))
}

func validate(opts *Opts) bool {
	if opts.DdlFile == "" {
		log.Print("Error: must supply a ddl file")
		return false
	}

	i, err := strconv.ParseInt(opts.Order, 0, 0)

	if err != nil {
		log.Print("Error: order must be an int")
		return false
	}

	opts.OrderInt = int(i)

	return true
}
