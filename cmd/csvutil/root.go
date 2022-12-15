package csvutil

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

const version = "0.0.3"

var threads int
var delimiter string
var rootCmd = &cobra.Command{
	Version: version,
	Use:     "csvutil",
	Short:   "Quickly perform simple operations on CSV files",
	Long:    `csvutil provides fast alternatives for wc, head, select and other operations for csv files`,
	Run:     func(cmd *cobra.Command, args []string) {},
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if threads < 1 {
			cmd.PrintErrln("Threads can't be less than 1")
			os.Exit(1)
		}

		if len(delimiter) != 1 {
			cmd.PrintErrln("Delimiter must be a single character")
			os.Exit(1)
		}
	},
}

func Execute() {
	rootCmd.CompletionOptions.HiddenDefaultCmd = true
	rootCmd.PersistentFlags().IntVarP(&threads, "threads", "t", 1, "Number of concurrent workers, using Stdin overrides this flag")
	rootCmd.PersistentFlags().StringVarP(&delimiter, "delimiter", "d", ",", "Choose delimiter")
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "csvutil encountered an error while executing")
		os.Exit(1)
	}
}
