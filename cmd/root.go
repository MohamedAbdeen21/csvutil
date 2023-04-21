package csvutil

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

const version = "0.0.4"

var (
	threads   int
	delimiter string
)

// function instead of a var like all other commands for testing
func RootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Version: version,
		Use:     "csvutil",
		Short:   "Quickly perform simple operations on CSV files",
		Long:    `csvutil provides fast alternatives for wc, head, select and other operations for csv files`,
		Run:     func(cmd *cobra.Command, args []string) {},
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if len(args) > 0 {
				if _, err := os.Stat(args[0]); err != nil {
					return fmt.Errorf("file %s doesn't exist", args[0])
				}
			}

			if threads < 1 {
				return fmt.Errorf("threads can't be less than 1")
			}

			if len(delimiter) != 1 {
				return fmt.Errorf("delimiter must be a single character")
			}
			return nil
		},
		SilenceUsage: true,
	}
	cmd.CompletionOptions.HiddenDefaultCmd = true
	cmd.PersistentFlags().IntVarP(&threads, "threads", "t", 1, "Number of concurrent workers")
	cmd.PersistentFlags().StringVarP(&delimiter, "delimiter", "d", ",", "Choose delimiter")
	cmd.AddCommand(statCmd())
	cmd.AddCommand(selectCmd())
	cmd.AddCommand(countCmd(&map[string]int64{}))
	cmd.AddCommand(plotCmd())
	return cmd
}
