package csvutil

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

const version = "0.0.1"

var threads int
var delimiter string
var rootCmd = &cobra.Command{
	Use:     "csvutil",
	Short:   "Quickly perform simple operations on CSV files",
	Long:    `csvutil provides fast alternatives for wc, head, select and other operations for csv files`,
	Version: version,
	Run:     func(cmd *cobra.Command, args []string) {},
}

func Execute() {
	rootCmd.PersistentFlags().IntVarP(&threads, "threads", "t", 1, "Number of concurrent workers")
	rootCmd.PersistentFlags().StringVarP(&delimiter, "delimiter", "d", ",", "Choose delimiter")
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "csvutil encountered an error while executing")
		os.Exit(1)
	}
}
