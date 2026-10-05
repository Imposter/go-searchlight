// Command slbench is Searchlight's benchmark harness against Elasticsearch 8.x (spec
// section 14), modelled on Rally:
//
//	slbench gen       generate a product dataset and saved-search sets (NDJSON)
//	slbench load      create the index on each engine and bulk-load the dataset
//	slbench run       load, cross-check, then run every workload on each engine
//	slbench report    render a run's JSON as markdown (docs/benchmarks.md)
//	slbench translate print the Elasticsearch DSL for a Searchlight query or search
//
// Searchlight runs as its real binary: --sl-bin starts it as a child process, on SQLite
// with a generated tokens file (internal/slproc), and restarts it for the restart
// workload; with --recovery-store-url (Postgres or MySQL) it also starts a two-node
// cluster of its own for the recovery workload. A node run elsewhere is reached by
// --sl-url (with --sl-token), and Elasticsearch by --es-url. Run "slbench <command> -h"
// for its flags.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

const usage = `usage: slbench <command> [flags]

commands:
  gen        generate a product dataset and saved-search sets (NDJSON)
  load       create the index on each engine and bulk-load the dataset
  run        load, cross-check, then run every workload on each engine
  report     render a run's JSON as markdown
  translate  print the Elasticsearch DSL for a Searchlight query or search (stdin)

Run "slbench <command> -h" for a command's flags.
`

func run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	var err error
	switch args[0] {
	case "gen":
		err = cmdGen(ctx, args[1:], stdout, stderr)
	case "load":
		err = cmdRun(ctx, args[1:], stdout, stderr, true)
	case "run":
		err = cmdRun(ctx, args[1:], stdout, stderr, false)
	case "report":
		err = cmdReport(args[1:], stdout)
	case "translate":
		err = cmdTranslate(args[1:], stdin, stdout)
	case "help", "-h", "--help":
		fmt.Fprint(stdout, usage)
		return 0
	default:
		fmt.Fprintf(stderr, "slbench: unknown command %q\n\n%s", args[0], usage)
		return 2
	}
	switch {
	case err == nil:
		return 0
	case errors.Is(err, flag.ErrHelp):
		return 0
	case errors.Is(err, errMismatch):
		fmt.Fprintln(stderr, "slbench:", err)
		return 3
	default:
		fmt.Fprintln(stderr, "slbench:", err)
		return 1
	}
}
