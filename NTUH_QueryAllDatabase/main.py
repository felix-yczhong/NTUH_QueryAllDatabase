import argparse
import datetime
import pathlib
import concurrent
import os
import enum

ref = 'hg38'
date_of_query = datetime.date.today().strftime("%Y%m%d")

Mviewer_loc = ''

mviewer_postfix = f'.{ref}_multianno.Q{date_of_query}_cli.mviewer.tsv'

class run_mode(enum.IntEnum):
    solo = 0
    trio = 1

def config_argparse():
    parser = argparse.ArgumentParser(description='call query all database in batch')
    parser.add_argument('path', type=str, help='path to target directory')
    parser.add_argument('-ref', choices=['hg19', 'hg38'], default='hg38')
    args = parser.parse_args()
    return args

def find_caseName_and_mode(path):
    solo_postfix = f'.{ref}_multianno.txt'
    trio_postfix = f'_A.{ref}_multianno.txt'

    for inner_entry in os.scandir(path / '7.ANNOVAR'):
        if inner_entry.is_file():
            if inner_entry.name.endswith(trio_postfix):
                mode = run_mode.trio
                case_name = inner_entry.name[:len(trio_postfix)]
                break
            elif inner_entry.name.endswith(solo_postfix):
                mode = run_mode.solo
                case_name = inner_entry.name[:len(solo_postfix)]
                break
    else:
        raise FileNotFoundError('cannot identify case name')
    return case_name, mode

def worker(command):
    os.system(command)

def main():
    args = config_argparse()
    print(args)

    solo_info = {
        'vep': {'dir_name': '5.VEP', 'postfix': '.vep.txt'},
        'nirvana': {'dir_name': '8.Nirvana', 'postfix': '.json.gz'},
        'input': {'dir_anme': '7.ANNOVAR', 'postfix': f'.{ref}_multianno.txt'},
        'intervar': {'dir_name': '16.InterVar', 'postfix': f'.{ref}_multianno.txt.intervar'},
        'expansionHunter': {'dir_name': '21.ExtensionHunter', 'postfix': '.HomRegions.tsv'},
        # 'automap': {'dir_name': '20.AutoMap', 'postfix': '.vcf'} # special case, 1 more folder layer
    }

    trio_info = {
        'vep': {'dir_name': '5.VEP', 'postfix': '.vep.txt'},
        'nirvana': {'dir_name': '8.Nirvana', 'postfix': '.json.gz'},
        'input': {'dir_name': '7.ANNOVAR', 'postfix': f'_A.{ref}_multianno.txt'},
        'father': {'dir_name': '7.ANNOVAR', 'postfix': f'_B.{ref}_multianno.txt'},
        'mother': {'dir_name': '7.ANNOVAR', 'postfix': f'_C.{ref}_multianno.txt'},
        'intervar': {'dir_name': '16.InterVar', 'postfix': f'.{ref}_multianno.txt.intervar'},
        'expansionHunter': {'dir_name': '21.ExtensionHunter', 'postfix': '.HomRegions.tsv'},
        # 'automap': {'dir_name': '20.AutoMap', 'postfix': '.vcf'} # special case, 1 more folder layer
    }
    
    path = pathlib.Path(os.path.abspath(args.path))

    if not path.exists():
        raise FileNotFoundError(f'{path} does not exist!')

    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = dict()
        for outer_entry in os.scandir(path):
            if outer_entry.is_dir():
                case_name, mode = find_caseName_and_mode(path)

                info = solo_info if mode == run_mode.solo else trio_info
                param_dict = dict()
                for arg, d in info.items():
                    param_dict[arg] = path / d['dir_name'] / f"{case_name}{d['postfix']}"
                param_dict['output'] = path / f"{case_name}{mviewer_postfix}"
                # param_dict['automap'] = path / '20.AutoMap' / 'folder_name' / f"{case_name}{automap_postfix}"

                if mode == run_mode.solo:
                    MviewerCommand = \
                        (f"{Mviewer_loc} "
                        f"--ref {ref} " 
                        f"--input {param_dict['input']} "
                        f"--output {param_dict['output']} "
                        f"--nirvana {param_dict['nirvana']} "
                        f"--vep {param_dict['vep']} "
                        f"--intervar {param_dict['intervar']} "
                        # f"--automap {param_dict['20.AutoMap']} "
                        f"--expansionHunter {param_dict['expansionHunter']} "
                        "--always-dump")
                elif mode == run_mode.trio:
                    MviewerCommand = \
                        (f"{Mviewer_loc} "
                        f"--ref {ref} " 
                        f"--input {param_dict['input']} "
                        f"--output {param_dict['output']} "
                        f"--nirvana {param_dict['nirvana']} "
                        f"--vep {param_dict['vep']} "
                        f"--intervar {param_dict['intervar']} "
                        # f"--automap {param_dict['20.AutoMap']} "
                        f"--expansionHunter {param_dict['expansionHunter']} "
                        f"--father {param_dict['father']} "
                        f"--mother {param_dict['mother']} "
                        "--always-dump")   

                future = executor.submit(worker, command=MviewerCommand)
                futures[future] = case_name

        for future in concurrent.futures.as_completed(futures):
            if future.result() != 0:
                print(f'{futures[future]} failed!')
            else:
                print(f'{futures[future]} completed!')

if __name__ == '__main__':
    main()