import rainbowbatch.kfio as kfio

from pathlib import Path
from pygit2 import Repository


def check_git_branch(branchname):
    git_branch = Repository(kfio.TOP_LEVEL_DIR /
                            'kf_wiki_content/').head.shorthand.strip()
    return git_branch == branchname


def check_has_uncommitted_git_changes():
    git_repo = Repository(kfio.TOP_LEVEL_DIR / 'kf_wiki_content/')
    return len(git_repo.status()) != 0
