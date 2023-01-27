package csvutil

import (
	"os"
	"os/exec"

	"github.com/spf13/cobra"
)

var pycmd *exec.Cmd
var plotType string
var subcommand string
var outputDir string
var pipe_read, pipe_write *os.File

var plotCmd = &cobra.Command{
	Use:   "plot",
	Short: "Plot results of the following subcommand",
	Long:  "Plot the results of the following subcommands",
	Run:   func(cmd *cobra.Command, args []string) {},

	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		// Check global flags -t and -d
		cmd.Root().PersistentPreRun(cmd, args)

		// given path is a directory
		if cmd.Flags().Changed("output") && outputDir[len(outputDir)-1] == '/' {
			panic("output must be a file not a directory")
		}

		pipe_read, pipe_write, _ = os.Pipe()
		cmd.SetOut(pipe_write)
		subcommand = cmd.Name()

		if !cmd.Flags().Changed("plot") {
			switch subcommand {
			case "count":
				plotType = "bar"
			case "select":
				plotType = "hist"
			case "line":
				plotType = "line"
			default:
				plotType = "scatter"
			}
		}

		pycmd = exec.Command("./plot")
		pipe_write.WriteString(plotType + "\n")
		pipe_write.WriteString(outputDir + "\n")
		pipe_write.WriteString(subcommand + "\n")
		pycmd.Stdin = pipe_read
		pycmd.Stdout = os.Stdout
		pycmd.Stderr = os.Stderr
		// start reading before actual function to avoid
		// filling the write buffer, will exit when
		// the write end is closed; in PostRun
		go pycmd.Start()
	},

	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		pipe_write.Close()
		pycmd.Wait()
	},
}

func init() {
	plotCmd.PersistentFlags().StringVarP(&plotType, "plot", "p", plotType, "The type of plot")
	plotCmd.PersistentFlags().StringVarP(&outputDir, "output", "o", "./plot.png", "Path of the generated plot")
	RootCmd.AddCommand(plotCmd)
}
