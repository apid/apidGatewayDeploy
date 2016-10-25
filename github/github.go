package github

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/30x/apid"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"regexp"
	"strings"
)

var log apid.LogService

func Init(services apid.Services) {
	log = services.Log()
}

func printResponseInfo(resp *http.Response) {
	d, _ := httputil.DumpResponse(resp, false)
	s := strings.Replace(string(d), "\n", "\n  ", -1)
	fmt.Printf("DumpResponse: %s", s)
}

func jsonPrettyPrint(in []byte) {
	var out bytes.Buffer
	err := json.Indent(&out, in, "", "\t")
	if err != nil {
		s := string(in)
		if len(s) > 512 {
			s = s[:512]
		}
		fmt.Printf("jsonPrettyPrint err: %v (raw snippet): %s\n", err, s)
	}
	fmt.Printf("jsonPrettyPrint: %s\n", out.String())
}

//  SaveContentFile() invokes GetContentFileData() and writes
//  to a destination file. Optional 'destFileName' param defaults to
//  content file name if empty string
func SaveContentFile(repo string, contentFilePath string, ref string, destDir string, destFileName string, accessToken string) (destFilePath string, err error) {
	data, err := GetContentFileData(repo, contentFilePath, ref, accessToken)
	if err != nil {
		log.Errorf("Failed getting content file data: %v", err)
		return
	}
	if err = os.MkdirAll(destDir, 0700); err != nil {
		log.Errorf("Failed dest directory creation: %v", err)
		return
	}
	defer data.Close()
	if destFileName == "" {
		x := strings.LastIndex(contentFilePath, "/")
		destFileName = contentFilePath[x+1:] // exclude leading slash
	}
	destFilePath = destDir + "/" + destFileName
	f, err := os.Create(destFilePath)
	if err != nil {
		log.Errorf("Failed dest file creation: %v", err)
	}

	if _, err := io.Copy(f, data); err != nil {
		log.Errorf("Failed dest file copy: %v", err)
	}
	return
}

/*  GetUrlData() validates GitHub URL and invokes GetContentFileData()
    to return file data as a ReadCloser, which must be closed.
*/
func GetUrlData(u *url.URL, accessToken string) (io.ReadCloser, error) {

	if accessToken == "" {
		return nil, fmt.Errorf("invalid accessToken: %v", accessToken)
	}

	regexStr := `^/(.*)/(blob|raw)/([^/]+)/(.*)$`
	validPath := regexp.MustCompile(regexStr)
	m := validPath.FindStringSubmatch(u.Path)
	if len(m) == 0 {
		return nil, errors.New("DownloadFileUrl Invalid URL path (" + u.Path + ") does not match regex: " + regexStr)
	}
	fmt.Printf("DownloadFileUrl: matches: %v\n", m)
	repo := m[1]
	branch := m[3]
	contentFilePath := m[4]
	return GetContentFileData(repo, contentFilePath, branch, accessToken)
}

//  GetContentFileData() uses GitHub OAuth access token to retrieve
//  metadata and returns a ReadCloser with file content data from a
//  GitHub public/private repo.
func GetContentFileData(repo string, contentFilePath string, ref string, accessToken string) (data io.ReadCloser, err error) {
	log.Debug("repo:", repo)
	log.Debug("contentFilePath:", contentFilePath)
	log.Debug("ref:", ref)

	x := strings.LastIndex(contentFilePath, "/")
	parentPath := contentFilePath[:x+1] // include trailing slash
	fileName := contentFilePath[x+1:]   // exclude leading slash
	log.Debug("parentPath:", parentPath)
	log.Debug("fileName:", fileName)

	//url := "https://api.github.com/repos/apigee-internal/apidx/contents/mockdata/kms/kms.db-shm?ref=master"
	url := "https://api.github.com/repos/" + repo + "/contents" + parentPath + "?ref=" + ref

	client := &http.Client{
	//CheckRedirect: redirectPolicyFunc,
	}
	req, err := http.NewRequest("GET", url, nil)
	if strings.HasPrefix(accessToken, "UPDATE_FROM_GITHUB_API") {
		log.Debug("GitHubAccessToken not configured, so omitting Authorization header")
	} else {
		req.Header.Add("Authorization", "token "+accessToken)
	}
	req.Header.Add("Accept", "application/vnd.github.v3.json")
	req.Header.Add("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		// handle error
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	printResponseInfo(resp)

	// pretty-print JSON output
	jsonPrettyPrint(body)

	// parse JSON response
	type GitHubContentFile struct {
		Name        string `json:"name"`
		Path        string `json:"path"`
		Sha         string `json:"sha"`
		Size        int32  `json:"size"`
		Url         string `json:"url"`
		Type        string `json:"type"`
		DownloadUrl string `json:"download_url"`
		// ...
	}
	var contents []GitHubContentFile
	err = json.Unmarshal(body, &contents)
	if err != nil {
		log.Errorf("Failed to parse JSON response: %v", err)
	}
	fmt.Printf("GitHubContentResp: %+v\n", contents)

	// find target content file info
	var downloadUrl string
	var contentPtr *GitHubContentFile
	for _, c := range contents {
		if c.Name == fileName {
			contentPtr = &c
			break
		}
	}
	if contentPtr == nil {
		log.Errorf("Error: no content found for: %s", contentFilePath)
	} else {
		fmt.Printf("Found %s with download_url: %s\n", fileName, contentPtr.DownloadUrl)
		downloadUrl = contentPtr.DownloadUrl
	}
	// download target content file
	resp2, err := client.Get(downloadUrl)
	if err != nil {
		log.Errorf("Error on download: %v", err)
		return
	}
	printResponseInfo(resp2)

	return resp2.Body, nil
}

//func main() {
//
//	log.Debug("GitHubGet start")
//	destDir := "./data"
//	accessToken := "abc123"
//
//	url, err := url.Parse("https://github.com/apigee-internal/apidx/blob/master/NOTICE")
//	if (err != nil) {
//		log.Errorf("Failed to parse URL: %v", err)
//		return
//	}
//
//	data, err := GetUrlData(url, accessToken)
//	if (err != nil) {
//		log.Errorf("Failed to get URL data: %v", err)
//		return
//	}
//	defer data.Close()
//
//	if newFile, err := SaveContentFile("apigee-internal/apidx", "/README.md", "master", destDir, "", accessToken); err == nil {
//		stat, _ := os.Stat(newFile)
//		fmt.Printf("SaveContentFile: %+v", stat)
//	} else {
//		log.Errorf("Failed to save content file: %v", err)
//	}
//
//	if newFile, err := SaveContentFile("apigee-internal/apidx", "/README.md", "master", destDir, "saved_file_1", accessToken); err == nil {
//		stat, _ := os.Stat(newFile)
//		fmt.Printf("SaveContentFile: %+v", stat)
//	} else {
//		log.Errorf("Failed to save content file: %v", err)
//	}
//
//}
