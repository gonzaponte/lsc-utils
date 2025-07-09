from pathlib import Path
from socket  import gethostname

LDCS     = list(range(1,8))
TRIGGERS = list(range(0,3))


def get_data_path(run):
    host = gethostname()
    if   host == "frontend1next" or host[:4] == "stor" or host[:8] == "analysis":
        return Path(f"/analysis/{run}/hdf5")
    elif host == "admin": # DESMAN
        return Path.home() / f"analysis/{run}/hdf5"
    else:
        raise RuntimeError(f"Unknown host: {host}")


def select_last_version_in_dir(path: Path):
    dirs = sorted(filter(Path.is_dir, path.glob("*")))
    if not len(dirs):
        raise RuntimeError(f"Directory {path} is empty!")
    return dirs[-1]


def select_last_version_city(path: Path, city: str):
    path_ic   = select_last_version_in_dir(path   )
    path_topi = select_last_version_in_dir(path_ic)
    return path_topi / city


def choose_version_in_dir(path: Path, label: str):
    vers = sorted(path.glob("*"))
    ver  = vers[0]
    if len(vers) == 1:
        return ver

    ans = ""
    while ans not in map(str, range(len(vers))):
        for i, ver in enumerate(vers):
            print(f"{i} => {ver}")
        ans = input(f"Choose a {label} version: ")
    return vers[int(ans)]


def choose_version_city(path: Path, city: str):
    icver   = choose_version( path, "IC"  )
    topiver = choose_version(icver, "TOPI")
    return topiver / city


def find_city_path(path: Path, city: str, use_last_version: bool):
    if use_last_version: return select_last_version_city(path, city)
    else               : return      choose_version_city(path, city)


def valid_trigger(trigger: str):
    if trigger not in list("012*"):
        raise ValueError(f"Unknown trigger: {trigger}")
    return trigger


def valid_ldc(ldc: str):
    if ldc not in list("1234567*"):
        raise ValueError(f"Unknown LDC: {ldc}")
    return ldc
