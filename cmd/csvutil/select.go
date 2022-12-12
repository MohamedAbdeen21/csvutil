package csvutil

import (
	"os"
	"strings"

	"github.com/MohamedAbdeen21/csvutil/pkg/csvutil"
	"github.com/spf13/cobra"
)

var columns_string string
var select_filters map[string]string
var limit int

var selectCmd = &cobra.Command{
	Use:     "select",
	Aliases: []string{"columns"},
	Short:   "Output chosen columns",
	Long:    "Output the columns specified by flag -c, if not specified all columns will be displayed. Specify -t 1 to preserve order of rows",
	Args:    cobra.RangeArgs(0, 1),
	Run: func(cmd *cobra.Command, args []string) {
		var columns []string

		if cmd.Flags().Changed("columns") {
			columns = strings.Split(columns_string, ",")
		}

		if len(args) == 0 {
			csvutil.Columns(os.Stdin.Name(), columns, select_filters, limit, 1, delimiter, os.Stdout)
		} else {
			csvutil.Columns(args[0], columns, select_filters, threads, limit, delimiter, os.Stdout)
		}
	},
}

func init() {
	selectCmd.Flags().StringVarP(&columns_string, "columns", "c", "", "Columns to output")
	selectCmd.Flags().StringToStringVarP(&select_filters, "filter", "f", map[string]string{}, "Filter where COLUMN=VALUE")
	selectCmd.Flags().IntVarP(&limit, "limit", "n", -1, "Limit number of printed rows")
	rootCmd.AddCommand(selectCmd)
}
