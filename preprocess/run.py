"""
Creator: Mariana
Date: Fev. 2022
After download the raw data we need to preprocessing it.
At the end of this stage we have been created a new artfiact (clean_data).
"""
import argparse
import logging
import os
import pandas as pd
import wandb

# configure logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(message)s",
                    datefmt='%d-%m-%Y %H:%M:%S')

# reference for a logging obj
logger = logging.getLogger()

def process_args(args):
    """
    Arguments
        args - command line arguments
        args.input_artifact: Fully qualified name for the raw data artifact
        args.artifact_name: Name for the W&B artifact that will be created
        args.artifact_type: Type of the artifact to create
        args.artifact_description: Description for the artifact
    """
    
    # create a new wandb project
    run = wandb.init(job_type="process_data")
    
    logger.info("Downloading artifact")
    artifact = run.use_artifact(args.input_artifact)
    artifact_path = artifact.file()
    
    # create a dataframe from the artifact path
    
    df = pd.read_csv(artifact_path, sep=',', low_memory=False)
    
    #droping columns we dont use
    df.drop(columns=['key', 'uri', 'track_href', 'analysis_url', 'id', 'time_signature',
                     'song_name', 'Unnamed: 0', 'title', 'type', 'mode', 'tempo', 'duration_ms'], inplace=True)
    
    #removing some genres, kepping only ['Rap', 'Pop', 'Hiphop', 'trance', 'trap']
    df=df[(df.genre!='Dark Trap') & (df.genre!='RnB') &
          (df.genre!='Underground Rap') & (df.genre!='psytrance') &
          (df.genre!='techhouse') & (df.genre!='Emo') &
          (df.genre!='dnb') & (df.genre!='hardstyle') &
          (df.genre!='Trap Metal') & (df.genre!='techno')]
    
    #getting a sample
    df=pd.concat([df[df.genre=='Rap'].sample(n=1000),
                    df[df.genre=='Pop'].sample(n=461),
                    df[df.genre=='Hiphop'].sample(n=1000),
                    df[df.genre=='trance'].sample(n=1000),
                    df[df.genre=='trap'].sample(n=1000)])
    
    print(df.info())
    # Generate a "clean data file"
    filename = "preprocessed_data.csv"
    df.to_csv(filename,index=False)
    
    # Create a new artifact and configure with the necessary arguments
    artifact = wandb.Artifact(
        name=args.artifact_name,
        type=args.artifact_type,
        description=args.artifact_description
    )
    artifact.add_file(filename)
    
    # Upload the artifact to Wandb
    logger.info("Logging artifact")
    run.log_artifact(artifact)

    # Remote temporary files
    os.remove(filename)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Preprocess a dataset",
        fromfile_prefix_chars="@"
    )

    parser.add_argument(
        "--input_artifact",
        type=str,
        help="Fully-qualified name for the input artifact",
        required=True
    )

    parser.add_argument(
        "--artifact_name", type=str, help="Name for the artifact", required=True
    )

    parser.add_argument(
        "--artifact_type", type=str, help="Type for the artifact", required=True
    )

    parser.add_argument(
        "--artifact_description",
        type=str,
        help="Description for the artifact",
        required=True
    )

    # get arguments
    ARGS = parser.parse_args()

    # process the arguments
    process_args(ARGS)
    
    
# mlflow run . -P input_artifact=spotify_mlops/spotify_mlops:latest -P artifact_name=data_preprocessed -P artifact_type=preprocess_data -P artifact_description="This is a Dataset of songs in Spotify preprocessed"