package csvutil

func countReducer(mappers []*Mapper, mode string) map[string]int64 {
	var result map[string]int64 = make(map[string]int64)
	switch mode {
	case "lines", "bytes":
		for _, mapper := range mappers {
			result["total"] += mapper.getCount()["total"]
		}

	case "group":
		for _, mapper := range mappers {
			for key, value := range mapper.getCount() {
				result[key] += value
			}
		}
	}
	return result
}

func statReducer(channels []chan string) map[string]int64 { return map[string]int64{} }
