# push lists to git

import base64
#from github import Github
#from github import InputGitTreeElement
from datetime import datetime

class pushToGit:
    def __init__(self, gitFolderPath, Username, Password):
        self.gitFolderPath=gitFolderPath
        self.username=Username
        self.password=Password

    def pushData(self):
        user = "lcia-projects"
        password = "2OrangeCats!"
        g = Github(user, password)
        repo = g.get_user().get_repo('cyber-intelligence-lists')
        file_list = [
            '/Users/darrellmiller/Dropbox/Fusion Projects/Current Projects/lookout_user_password_pull/Output/password.keyword.csv',
            '/Users/darrellmiller/Dropbox/Fusion Projects/Current Projects/lookout_user_password_pull/Output/password.keyword.txt',
            '/Users/darrellmiller/Dropbox/Fusion Projects/Current Projects/lookout_user_password_pull/Output/username.keyword.csv',
            '/Users/darrellmiller/Dropbox/Fusion Projects/Current Projects/lookout_user_password_pull/Output/username.keyword.txt'

        ]

        file_names = [
            'password.keyword.csv',
            'password.keyword.txt',
            'username.keyword.csv',
            'username.keyword.txt'
        ]
        commit_message = 'Updated Data' + datetime.now
        master_ref = repo.get_git_ref('heads/master')
        master_sha = master_ref.object.sha
        base_tree = repo.get_git_tree(master_sha)
        element_list = list()
        for i, entry in enumerate(file_list):
            with open(entry) as input_file:
                data = input_file.read()
            if entry.endswith('.png'):
                data = base64.b64encode(data)
            element = InputGitTreeElement(file_names[i], '100644', 'blob', data)
            element_list.append(element)
        tree = repo.create_git_tree(element_list, base_tree)
        parent = repo.get_git_commit(master_sha)
        commit = repo.create_git_commit(commit_message, tree, [parent])
        master_ref.edit(commit.sha)