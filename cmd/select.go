package csvutil

import (
	"math"
	"os"
	"strings"

	csvutil "github.com/MohamedAbdeen21/csvutil/pkg"
	"github.com/spf13/cobra"
)

var columns_string string
var select_filters map[string]string
var limit int
var keepHeaders bool

func selectCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "select",
		Short:   "Output chosen columns",
		Long:    "Output the columns specified by flag -c, if not specified all columns will be displayed. Choose -t 1 to preserve order of rows.\nFiltering is done before the limit. To limit before filtering, consider piping the output of `head` command.",
		Example: "csvutil select file.csv -c name,age -f age=20 -n 20 --headers=false\ncsvutil select file.csv -c age,name > reordered_file.csv ",
		Args:    cobra.RangeArgs(0, 1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var columns []string

			if cmd.Flags().Changed("columns") {
				columns = strings.Split(columns_string, ",")
			}

			filter := make(map[string][]string)
			for key, value := range select_filters {
				values := strings.Split(value, "||")
				filter[key] = values
			}

			option := csvutil.Options{
				Columns:     columns,
				Filters:     filter,
				KeepHeaders: keepHeaders,
				Limit:       limit,
				Output:      cmd.OutOrStdout(),
				Delimiter:   delimiter,
				Threads:     threads,
			}

			if len(args) == 0 {
				fd := csvutil.CopyToTemp(cmd.InOrStdin())
				defer os.Remove(fd.Name())
				defer fd.Close()
				option.Filename = fd.Name()
			} else {
				option.Filename = args[0]
			}
			err := csvutil.Columns(&option)

			if err != nil {
				return err
			}

			return nil
		},
	}
	cmd.Flags().StringVarP(&columns_string, "columns", "c", "", "Columns to output")
	cmd.Flags().
		StringToStringVarP(&select_filters, "filter", "f", map[string]string{}, "Filter where COLUMN=\"VALUE1||VALUE2||...\"")
	cmd.Flags().IntVarP(&limit, "limit", "n", math.MaxInt, "Limit number of printed rows")
	// can't use shorthand h, reserved for --help
	cmd.Flags().
		BoolVar(&keepHeaders, "headers", true, "Set to =false to skip header row")
	return cmd
}
