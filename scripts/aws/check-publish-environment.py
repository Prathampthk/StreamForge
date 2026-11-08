#!/usr/bin/env python3
"""Read-only verification of the GitHub environment before requesting AWS credentials."""
import json,os,re
from urllib.request import Request,urlopen
repo=os.environ['GITHUB_REPOSITORY'];token=os.environ['GITHUB_TOKEN']
if not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+',repo):raise RuntimeError('invalid repository')
def get(path):
 with urlopen(Request('https://api.github.com/repos/'+repo+path,headers={'Authorization':'Bearer '+token,'Accept':'application/vnd.github+json'}),timeout=10) as r:return json.load(r)
env=get('/environments/aws-dev-publish')
policy=env.get('deployment_branch_policy') or {}
if policy.get('custom_branch_policies'):
 branches=get('/environments/aws-dev-publish/deployment-branch-policies')['branch_policies']
 if len(branches)!=1 or branches[0]['name']!='main' or branches[0].get('type','branch')!='branch':raise RuntimeError('environment must allow only main')
elif policy.get('protected_branches'):
 if not get('/branches/main')['protected']:raise RuntimeError('main must be protected')
else:raise RuntimeError('environment branch restrictions required')
print('PASS branch-restricted publish environment; no AWS request made')
