package csvutil

import (
	"os"
	"strings"

	csvutil "github.com/MohamedAbdeen21/csvutil/pkg"
	"github.com/spf13/cobra"
)

var plotSelectCmd = &cobra.Command{
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
			cmd.Print(">")
			csvutil.Columns(&csvutil.Options{
				Filename:    os.Stdin.Name(),
				Columns:     columns,
				Filters:     select_filters,
				Threads:     1,
				KeepHeaders: true,
				Limit:       limit,
				Delimiter:   delimiter,
				Output:      pipe_write,
			})
		} else {
			csvutil.Columns(&csvutil.Options{
				Filename:    args[0],
				Columns:     columns,
				Filters:     select_filters,
				Threads:     threads,
				KeepHeaders: true,
				Limit:       limit,
				Delimiter:   delimiter,
				Output:      pipe_write,
			})
		}
	},
}

func init() {
	plotSelectCmd.Flags().StringVarP(&columns_string, "columns", "c", "", "Columns to output")
	plotSelectCmd.Flags().StringToStringVarP(&select_filters, "filter", "f", map[string]string{}, "Filter where COLUMN=VALUE")
	plotSelectCmd.Flags().IntVarP(&limit, "limit", "n", -1, "Limit number of printed rows")
	plotCmd.AddCommand(plotSelectCmd)
}
