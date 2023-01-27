package csvutil

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

const version = "0.0.4"

var threads int
var delimiter string
var RootCmd = &cobra.Command{
	Version: version,
	Use:     "csvutil",
	Short:   "Quickly perform simple operations on CSV files",
	Long:    `csvutil provides fast alternatives for wc, head, select and other operations for csv files`,
	Run:     func(cmd *cobra.Command, args []string) {},
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if len(args) > 0 && cmd.Name() != "stat" {
			if _, err := os.Stat(args[0]); err != nil {
				cmd.PrintErrf("file %s doesn't exists\n", args[0])
				os.Exit(1)
			}
		}

		if threads < 1 {
			cmd.PrintErrln("threads can't be less than 1")
			os.Exit(1)
		}

		if len(delimiter) != 1 {
			cmd.PrintErrln("delimiter must be a single character")
			os.Exit(1)
		}
	},
}

func Execute() {
	RootCmd.CompletionOptions.HiddenDefaultCmd = true
	RootCmd.PersistentFlags().IntVarP(&threads, "threads", "t", 1, "Number of concurrent workers, using Stdin overrides this flag")
	RootCmd.PersistentFlags().StringVarP(&delimiter, "delimiter", "d", ",", "Choose delimiter")
	if err := RootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "csvutil encountered an error while executing")
		os.Exit(1)
	}
}
