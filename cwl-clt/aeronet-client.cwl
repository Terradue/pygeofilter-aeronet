# Copyright 2025 Terradue
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cwlVersion: v1.2

class: CommandLineTool
id: aeronet-client
label: AERONET Client Tool
doc: |
  This tool uses the AERONET Client to retrieve AERONET data based on specified search criteria.
hints:
  - class: DockerRequirement
    dockerPull: ghcr.io/terradue/aeronet-client:0.4.0
requirements:
  - class: InlineJavascriptRequirement
  - class: NetworkAccess
    networkAccess: true
  - class: SchemaDefRequirement
    types:
    - $import: https://raw.githubusercontent.com/eoap/schemas/main/string_format.yaml
    - $import: https://raw.githubusercontent.com/Terradue/pygeofilter-aeronet/refs/heads/main/schemas/search.yaml

inputs:
  search_request:
      label: AERONET client request settings
      doc: AERONET client request settings for data retrieval
      type: |-
        https://raw.githubusercontent.com/Terradue/pygeofilter-aeronet/refs/heads/main/schemas/search.yaml#AeronetSearchSettings

outputs:
  search_output:
    type: File
    outputBinding:
      glob: output.geoparquet

baseCommand: ["aeronet-client"]
arguments:
  - "search"
  - --format
  - geoparquet
  - --output-file
  - output.geoparquet
  - ${
      const args = [];
      const filter = inputs.search_request?.filter;
      const filterLang = inputs.search_request?.['filter-lang'];
      if (filterLang) {
        args.push('--filter-lang', filterLang);
      }
      if (filter) {
        args.push('--filter', JSON.stringify(filter));
      }
      return args;
    }
  