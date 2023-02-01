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

		option := csvutil.Options{
			Columns:     columns,
			Filters:     select_filters,
			KeepHeaders: keepHeaders,
			Limit:       limit,
			Output:      pipe_write,
			Delimiter:   delimiter,
			Threads:     threads,
		}

		if len(args) == 0 {
			fd := csvutil.CopyToTemp(os.Stdin)
			defer fd.Close()
			defer os.Remove(fd.Name())
			option.Filename = fd.Name()
		} else {
			option.Filename = args[0]
		}

		err := csvutil.Columns(&option)

		if err != nil {
			cmd.PrintErrln(err.Error())
			os.Exit(1)
		}
	},
}

func init() {
	plotSelectCmd.Flags().StringVarP(&columns_string, "columns", "c", "", "Columns to output")
	plotSelectCmd.Flags().StringToStringVarP(&select_filters, "filter", "f", map[string]string{}, "Filter where COLUMN=VALUE")
	plotSelectCmd.Flags().IntVarP(&limit, "limit", "n", -1, "Limit number of printed rows")
	plotCmd.AddCommand(plotSelectCmd)
}
