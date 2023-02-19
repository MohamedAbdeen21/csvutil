package csvutil

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/MohamedAbdeen21/csvutil/pkg/utility"
	"github.com/spf13/cobra"
)

var plotType string
var outputDir string

func plotCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "plot",
		Short: "Plot results of the following subcommand",
		Long:  "Plot the results of the following subcommands",
		Run:   func(cmd *cobra.Command, args []string) {},

		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			// Check global flags -t and -d
			err := cmd.Root().PersistentPreRunE(cmd, args)
			if err != nil {
				return err
			}

			if !utility.ExistsIn(strings.ToLower(plotType), plotPossibleTypes) {
				return fmt.Errorf("%s is not supported, use one of %v", plotType, plotPossibleTypes)
			}

			if cmd.Flags().Changed("output") {
				if outputDir[len(outputDir)-1] == '/' {
					outputDir += "plot.html"
				} else if filepath.Ext(outputDir) != ".html" {
					outputDir += ".html"
				}
			}

			return nil
		},
	}
	cmd.PersistentFlags().
		StringVarP(&plotType, "plot", "p", "bar", "The type of plot: Line, Bar, Scatter")
	cmd.PersistentFlags().
		StringVarP(&outputDir, "output", "o", "./plot.html", "Path of the generated plot in .html extension")
	cmd.AddCommand(plotCountCmd())
	return cmd
}
