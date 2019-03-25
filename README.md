# Elastic Browser Directory Index

This readme describes the pipline to create the directory index in elasticsearch which
serves the backend of the elastic browser.

## The Moles catalogue mapping

Graham Parton has created a script to extract the mapping for all datasets, including
unpublished.

## Configuration

conf/config.ini

    [files]
    processing-directory = ****
    status-directory = ****
    missing-metadata-file = missing_metadata.txt
    moles-mapping = moles_catalogue_mapping.json
    
    [elasticsearch]
    es-host = https://jasmin-es1.ceda.ac.uk
    es-index = ceda-dirs
    es-user = ****
    es-password = ****

|Variable Name          | Description |
|-----------------------|-------------|
|processing-directory   | Directory to put lists of basic directory information and readme JSON files |
|status-directory       | Directory to put the current status for the update script |
|missing-metadata-file  | Name of file which lists all the directories missing MOLES metadata |
|moles-mapping          | Name of file which contains the MOLES mapping |
|es-host                | Elasticsearch host to send index to |
|es-index               | Elasticsearch index name to modify |
|es-user                | Elastisearch user for authentication to write |
|es-password            | Elasticsearch password for authentication to write |

## How to build the index
1. 
    
    `python create_dir_index/scripts/lotus_submit.py --config <config> --generate-dirs  [--dev]`
    
    Required:
        --config            Path to the config file
        --generate-dirs     Tells it which script to run
    
    Options:
        --dev               Tells the script to run on localhost, not submit to lotus 

    Creates:
        
    - File containing JSON strings \n separated for each of the spot file lists.
    - File containing 00readme content 

2. 
    `python create_dir_index/scripts/index_dirs.py --config <config>`
    
    Required:
        --config            Path to the config file
    
    Generates list of files which are missing MOLES metadata and pushes dirs with metadata to the specified index.
    Files missing MOLES metadata are output to file names in config file by `missing-metadata-file`
    
3. 
    `python create_dir_index/scripts/index_missing_metadata.py --config <config>`
    
    Required:
    --config            Path to the config file
    
    Tries a top down approad via the MOLES api to get metadata. Anything it can attribute
    is sent to the index and the remainder is outputted to file. 'reduced_missing.txt'

4. `python create_dir_index/scripts/update_readmes.py --config <config>`

    Required:
    --config            Path to the config file

    Updates the index with content from the 00readme files.
       
## Maintaining the index

The index is maintained by a cron job running on ingest2

`python create_dir_index/scripts/update_ceda_dirs.py --config <config>`

Required:
--config            Path to the config file
