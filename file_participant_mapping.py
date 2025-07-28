import pandas as pd
import argparse
import yaml



def file_participant_mapping(file_tsv_path, participant_tsv_path, csv_data_path, output_path):
    """
    Maps file data to participant data and generates an output TSV file with combined information.
    
    Parameters:
    - file_tsv_path: Path to the file TSV.
    - participant_tsv_path: Path to the participant TSV.
    - csv_data_path: Path to the CSV data containing participant information.
    - output_path: Path where the output TSV will be saved.
    """
    # Read the TSV and CSV files into DataFrames
    file_data = pd.read_csv(file_tsv_path, sep='\t')
    participant_data = pd.read_csv(participant_tsv_path, sep='\t')
    csv_data = pd.read_csv(csv_data_path)
    #csv_data = pd.read_excel(csv_data_path)
    csv_data['participant_id'] = csv_data['participant_id'].astype(str)
    participant_data['participant_id'] = participant_data['participant_id'].astype(str)
    #output_data = pd.DataFrame(columns=csv_data.columns)
    output_rows = []
    for index, row in file_data.iterrows():
        try:
            participant_row = participant_data.loc[participant_data['study_participant_id'] == row['participant.study_participant_id']].iloc[0]
        except IndexError:
            print(f"File with study_participant_id {row['participant.study_participant_id']} not found in participant data.")
            continue
        csv_data_row = csv_data.loc[csv_data['participant_id'] == participant_row['participant_id']].iloc[0]
        csv_data_row['guid'] = row['file_id']
        csv_data_row['md5'] = row['md5sum']
        csv_data_row['size'] = row['file_size']
        csv_data_row['urls'] = row['file_url_in_cds']
        consent_number = str(csv_data_row['consent_group_number'])
        #consent_number = str(csv_data_row['consent_code'])
        csv_data_row['acl'] = f"['phs001524.c{consent_number}']"
        csv_data_row['authz'] = f"['/project/phs001524.c{consent_number}']"
        # add row to output dataFrame
        output_rows.append(csv_data_row)   

    # Save the output DataFrame to a CSV file
    #output_data = pd.DataFrame(output_rows, columns=output_rows[0].keys())
    output_data = pd.DataFrame(output_rows, columns=csv_data.columns)
    for index, row in csv_data.iterrows():
        if row["participant_id"] not in output_data["participant_id"].values:
            print(f"Participant ID {row['participant_id']} not found in output data.")


    output_data.to_csv(output_path, sep='\t', index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Map file data to participant data and generate output TSV.")
    parser.add_argument('config_file', help='Confguration file', nargs='?', default=None)
    args = parser.parse_args()
    with open(args.config_file, "r") as f:
        config = yaml.safe_load(f)
    file_participant_mapping(config['file_tsv_path'], config['participant_tsv_path'], config['csv_data_path'], config['output_path'])