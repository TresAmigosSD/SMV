import argparse
import dulwich.porcelain

class GitHelper(object):
    """Git helper for initializing git repository
        and committing all unstaged and untracked files.
    """
    def __init__(self, repo_dir):
        self.repo_dir = repo_dir

    def init_repo(self):
        dulwich.porcelain.init(self.repo_dir)
        self.commit_unstaged_and_untracked("init git repository")

    def commit_unstaged_and_untracked(self, commit_msg=""):
        repo = dulwich.porcelain.open_repo(self.repo_dir)
        status = dulwich.porcelain.status(repo)
        repo.stage(status.unstaged + status.untracked)
        dulwich.porcelain.commit(repo, commit_msg)

def parse_args():
    parser = argparse.ArgumentParser(
        usage="python git_helper.py -o init/commit -d ~/repo_dir -m 'commit_msg'",
    )

    parser.add_argument('-o', dest='op', help="Operation: either 'init' or 'commit'")
    parser.add_argument('-d', dest='repo_dir', default=".", help="Git repo directory")
    parser.add_argument('-m', dest='commit_msg', default="", help="Git commit message")

    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()

    op = args.op
    if op in ["init", "commit"]:
        gitHelper = GitHelper(args.repo_dir)
        if op == "init":
            gitHelper.init_repo()
        else:
            gitHelper.commit_unstaged_and_untracked(args.commit_msg)
    else:
        raise Exception("Invalid op: {}, only 'init' and 'commit' are valid!".format(op))
