package fs

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

func generateTreeCommand(path string) string {
	return fmt.Sprintf("tree -f --dirsfirst -i -s -D -a -J --timefmt '%%Y-%%m-%%d %%H:%%M:%%S' %s", path)
}

func parseTreeOutput(output []byte, basePath string) ([]EntryInfo, error) {
	var result []map[string]any
	if err := json.Unmarshal(output, &result); err != nil {
		return nil, err
	}

	var entries []EntryInfo
	if len(result) > 0 {
		parseTreeItems(result[0], basePath, &entries)
	}

	return entries, nil
}

func parseTreeItems(node map[string]any, basePath string, entries *[]EntryInfo) {
	if contents, ok := node["contents"].([]any); ok {
		for _, item := range contents {
			if itemMap, ok := item.(map[string]any); ok {
				entry := parseTreeItem(itemMap, basePath)
				*entries = append(*entries, entry)

				if entry.IsDir {
					parseTreeItems(itemMap, basePath, entries)
				}
			}
		}
	}
}

func parseTreeItem(item map[string]any, basePath string) EntryInfo {
	path := ""
	if name, ok := item["name"].(string); ok {
		path = strings.TrimPrefix(name, basePath)
		path = strings.TrimPrefix(name, "/")
	}

	isDir := false
	if itemType, ok := item["type"].(string); ok {
		isDir = itemType == "directory"
		if isDir {
			path = strings.TrimSuffix(path, "/") + "/"
		}
	}

	var size int64
	if sizeVal, ok := item["size"].(float64); ok {
		size = int64(sizeVal)
	}

	var lastModified int64
	if timeStr, ok := item["time"].(string); ok {
		if parsedTime, err := time.Parse("2006-01-02 15:04:05", timeStr); err == nil {
			lastModified = parsedTime.Unix()
		}
	}

	return EntryInfo{
		Path:         path,
		Size:         size,
		LastModified: lastModified,
		IsDir:        isDir,
	}
}
