// Copyright 2018 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"os"
	"strings"
	"testing"

	"github.com/kubeflow/pipelines/backend/src/apiserver/common"
	"github.com/stretchr/testify/assert"
)

// makeGzipTarball builds a gzip-compressed tarball containing a single entry
// with the given name and content. Used to construct decompression-bomb inputs
// whose compressed size is small but decompressed size is large.
func makeGzipTarball(t *testing.T, name string, content []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gzipWriter := gzip.NewWriter(&buf)
	tarWriter := tar.NewWriter(gzipWriter)
	err := tarWriter.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: int64(len(content))})
	assert.Nil(t, err)
	_, err = tarWriter.Write(content)
	assert.Nil(t, err)
	assert.Nil(t, tarWriter.Close())
	assert.Nil(t, gzipWriter.Close())
	return buf.Bytes()
}

// makeZip builds a zip archive containing a single entry with the given name
// and content.
func makeZip(t *testing.T, name string, content []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zipWriter := zip.NewWriter(&buf)
	writer, err := zipWriter.Create(name)
	assert.Nil(t, err)
	_, err = writer.Write(content)
	assert.Nil(t, err)
	assert.Nil(t, zipWriter.Close())
	return buf.Bytes()
}

func TestLoadFile(t *testing.T) {
	file := "12345"
	bytes, err := loadFile(strings.NewReader(file), 5)
	assert.Nil(t, err)
	assert.Equal(t, []byte(file), bytes)
}

func TestLoadFile_ExceedSizeLimit(t *testing.T) {
	file := "12345"
	_, err := loadFile(strings.NewReader(file), 4)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "File size too large")
}

func TestLoadFile_LargeDoc(t *testing.T) {
	bytes, _ := os.ReadFile("test/xgboost_sample_pipeline.yaml")
	file := string(bytes)
	readBytes, err := loadFile(strings.NewReader(file), common.MaxFileLength)
	assert.Nil(t, err)
	assert.Equal(t, bytes, readBytes)
}

func TestDecompressPipelineTarball(t *testing.T) {
	tarballByte, _ := os.ReadFile("test/arguments_tarball/arguments.tar.gz")
	pipelineFile, err := DecompressPipelineTarball(tarballByte)
	assert.Nil(t, err)

	expectedPipelineFile, _ := os.ReadFile("test/arguments-parameters.yaml")
	assert.Equal(t, expectedPipelineFile, pipelineFile)
}

func TestDecompressPipelineTarball_MalformattedTarball(t *testing.T) {
	tarballByte, _ := os.ReadFile("test/malformatted_tarball.tar.gz")
	_, err := DecompressPipelineTarball(tarballByte)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "Not a valid tarball file")
}

func TestDecompressPipelineTarball_NonYamlTarball(t *testing.T) {
	tarballByte, _ := os.ReadFile("test/non_yaml_tarball/non_yaml_tarball.tar.gz")
	_, err := DecompressPipelineTarball(tarballByte)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "Expecting a pipeline.yaml file inside the tarball")
}

func TestDecompressPipelineTarball_EmptyTarball(t *testing.T) {
	tarballByte, _ := os.ReadFile("test/empty_tarball/empty.tar.gz")
	_, err := DecompressPipelineTarball(tarballByte)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "Not a valid tarball file")
}

func TestDecompressPipelineTarball_DecompressionBomb(t *testing.T) {
	// A small, highly compressible archive whose decompressed pipeline.yaml
	// exceeds MaxFileLength must be rejected without buffering the whole entry.
	bomb := makeGzipTarball(t, "pipeline.yaml", bytes.Repeat([]byte("a"), common.MaxFileLength+1))
	assert.Less(t, len(bomb), common.MaxFileLength, "compressed bomb should be small")
	_, err := DecompressPipelineTarball(bomb)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "size too large")
}

func TestDecompressPipelineTarball_AtSizeLimit(t *testing.T) {
	content := bytes.Repeat([]byte("a"), common.MaxFileLength)
	archive := makeGzipTarball(t, "pipeline.yaml", content)
	decompressed, err := DecompressPipelineTarball(archive)
	assert.Nil(t, err)
	assert.Equal(t, content, decompressed)
}

func TestDecompressPipelineZip(t *testing.T) {
	zipByte, _ := os.ReadFile("test/arguments_zip/arguments-parameters.zip")
	pipelineFile, err := DecompressPipelineZip(zipByte)
	assert.Nil(t, err)

	expectedPipelineFile, _ := os.ReadFile("test/arguments-parameters.yaml")
	assert.Equal(t, expectedPipelineFile, pipelineFile)
}

func TestDecompressPipelineZip_DecompressionBomb(t *testing.T) {
	bomb := makeZip(t, "pipeline.yaml", bytes.Repeat([]byte("a"), common.MaxFileLength+1))
	assert.Less(t, len(bomb), common.MaxFileLength, "compressed bomb should be small")
	_, err := DecompressPipelineZip(bomb)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "size too large")
}

func TestDecompressPipelineZip_AtSizeLimit(t *testing.T) {
	content := bytes.Repeat([]byte("a"), common.MaxFileLength)
	archive := makeZip(t, "pipeline.yaml", content)
	decompressed, err := DecompressPipelineZip(archive)
	assert.Nil(t, err)
	assert.Equal(t, content, decompressed)
}

func TestDecompressPipelineZip_MalformattedZip(t *testing.T) {
	zipByte, _ := os.ReadFile("test/malformatted_zip.zip")
	_, err := DecompressPipelineZip(zipByte)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "Not a valid zip file")
}

func TestDecompressPipelineZip_MalformedZip2(t *testing.T) {
	zipByte, _ := os.ReadFile("test/malformed_zip2.zip")
	_, err := DecompressPipelineZip(zipByte)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "Not a valid zip file")
}

func TestDecompressPipelineZip_NonYamlZip(t *testing.T) {
	zipByte, _ := os.ReadFile("test/non_yaml_zip/non_yaml_file.zip")
	_, err := DecompressPipelineZip(zipByte)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "Expecting a pipeline.yaml file inside the zip")
}

func TestDecompressPipelineZip_EmptyZip(t *testing.T) {
	zipByte, _ := os.ReadFile("test/empty_tarball/empty.zip")
	_, err := DecompressPipelineZip(zipByte)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "Not a valid zip file")
}

func TestReadPipelineFile_YAML(t *testing.T) {
	file, _ := os.Open("test/arguments-parameters.yaml")
	fileBytes, err := ReadPipelineFile("arguments-parameters.yaml", file, common.MaxFileLength)
	assert.Nil(t, err)

	expectedFileBytes, _ := os.ReadFile("test/arguments-parameters.yaml")
	assert.Equal(t, expectedFileBytes, fileBytes)
}

func TestReadPipelineFile_JSON(t *testing.T) {
	file, _ := os.Open("test/v2-hello-world.json")
	fileBytes, err := ReadPipelineFile("v2-hello-world.json", file, common.MaxFileLength)
	assert.Nil(t, err)

	expectedFileBytes, _ := os.ReadFile("test/v2-hello-world.json")
	assert.Equal(t, expectedFileBytes, fileBytes)
}

func TestReadPipelineFile_Zip(t *testing.T) {
	file, _ := os.Open("test/arguments_zip/arguments-parameters.zip")
	pipelineFile, err := ReadPipelineFile("arguments-parameters.zip", file, common.MaxFileLength)
	assert.Nil(t, err)

	expectedPipelineFile, _ := os.ReadFile("test/arguments-parameters.yaml")
	assert.Equal(t, expectedPipelineFile, pipelineFile)
}

func TestReadPipelineFile_Zip_AnyExtension(t *testing.T) {
	file, _ := os.Open("test/arguments_zip/arguments-parameters.zip")
	pipelineFile, err := ReadPipelineFile("arguments-parameters.pipeline", file, common.MaxFileLength)
	assert.Nil(t, err)

	expectedPipelineFile, _ := os.ReadFile("test/arguments-parameters.yaml")
	assert.Equal(t, expectedPipelineFile, pipelineFile)
}

func TestReadPipelineFile_MultifileZip(t *testing.T) {
	file, _ := os.Open("test/pipeline_plus_component/pipeline_plus_component.zip")
	pipelineFile, err := ReadPipelineFile("pipeline_plus_component.ai-hub-package", file, common.MaxFileLength)
	assert.Nil(t, err)

	expectedPipelineFile, _ := os.ReadFile("test/pipeline_plus_component/pipeline.yaml")
	assert.Equal(t, expectedPipelineFile, pipelineFile)
}

func TestReadPipelineFile_Tarball(t *testing.T) {
	file, _ := os.Open("test/arguments_tarball/arguments.tar.gz")
	pipelineFile, err := ReadPipelineFile("arguments.tar.gz", file, common.MaxFileLength)
	assert.Nil(t, err)

	expectedPipelineFile, _ := os.ReadFile("test/arguments-parameters.yaml")
	assert.Equal(t, expectedPipelineFile, pipelineFile)
}

func TestReadPipelineFile_Tarball_AnyExtension(t *testing.T) {
	file, _ := os.Open("test/arguments_tarball/arguments.tar.gz")
	pipelineFile, err := ReadPipelineFile("arguments.pipeline", file, common.MaxFileLength)
	assert.Nil(t, err)

	expectedPipelineFile, _ := os.ReadFile("test/arguments-parameters.yaml")
	assert.Equal(t, expectedPipelineFile, pipelineFile)
}

func TestReadPipelineFile_MultifileTarball(t *testing.T) {
	file, _ := os.Open("test/pipeline_plus_component/pipeline_plus_component.tar.gz")
	pipelineFile, err := ReadPipelineFile("pipeline_plus_component.ai-hub-package", file, common.MaxFileLength)
	assert.Nil(t, err)

	expectedPipelineFile, _ := os.ReadFile("test/pipeline_plus_component/pipeline.yaml")
	assert.Equal(t, expectedPipelineFile, pipelineFile)
}

func TestReadPipelineFile_UnknownFileFormat(t *testing.T) {
	file, _ := os.Open("test/unknown_format.foo")
	_, err := ReadPipelineFile("unknown_format.foo", file, common.MaxFileLength)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "Unexpected pipeline file format")
}

func TestReadPipelineFile_SizeTooLarge_RecommendationIncluded(t *testing.T) {
	big := strings.Repeat("X", 1024)
	_, err := ReadPipelineFile("large.yaml", strings.NewReader(big), 10)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "File size too large")
	assert.Contains(t, err.Error(), "Consider moving large embedded artifacts or notebooks")
}
