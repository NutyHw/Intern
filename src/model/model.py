import mysql.connector 
from collections import defaultdict
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd

def connector( project_name : str ):
    mydb = mysql.connector.connect( 
        host='158.108.32.59',
        user='root',
        password='1q2w3e4r',
        database=project_name
    )
    return mydb

def run_op_code( project_name : str, op_code : str ) -> defaultdict:
    db = connector( project_name )
    cursor = db.cursor()
    cursor = cursor.execute( op_code, multi=True )

    res = dict()
    for records in cursor:
        records = records.fetchall()
        res = dict(records)

    return defaultdict(lambda : None,res)

def get_components( project_name : str, release_date : datetime ):
    lower_date_bound = release_date.strftime('%Y-%m-%d %H:%M:%S')
    upper_date_bound = ( release_date + relativedelta( months = 6 )).strftime('%Y-%m-%d %H:%M:%S')
    op_code = f'''
    select distinct(Component) from Commit
    where CommitterDate between '{ lower_date_bound }' and '{ upper_date_bound }'
    '''

    db = connector( project_name )
    cursor = db.cursor()
    cursor.execute( op_code )
    res = cursor.fetchall()
    return [ record[0] for record in res ]
    
# cal dependent variable
def count_post_release_defect( project_name : str, release_date : datetime ) -> defaultdict:
    lower_date_bound = release_date.strftime('%Y-%m-%d %H:%M:%S')
    upper_date_bound = ( release_date + relativedelta( months = 6 )).strftime('%Y-%m-%d %H:%M:%S')
    op_code = f'''
    select Component, count(*) from Commit
    where ChangeType = 'Fix' and CommitterDate between '{ lower_date_bound }' and '{ upper_date_bound }'
    group by Component;
    '''

    return run_op_code( project_name, op_code )

# product metrics cal function
def count_loc( project_name : str, release_date : datetime ) -> defaultdict:
    lower_date_bound = release_date.strftime('%Y-%m-%d %H:%M:%S')
    upper_date_bound = ( release_date + relativedelta( months = 6 )).strftime('%Y-%m-%d %H:%M:%S')
    op_code = f'''
    select a.Component, sum(a.LOC) from Commit a
    inner join ( 
        select File, max(CommitterDate) as CommitterDate from Commit
        where CommitterDate < '{ lower_date_bound }' 
        group by File
    ) b on b.File = a.File and b.CommitterDate = a.CommitterDate
    where a.LOC > 0
    group by a.Component;
    '''

    return run_op_code( project_name, op_code )

def cal_complexity( project_name : str, release_date : datetime ) -> defaultdict:
    lower_date_bound = release_date.strftime('%Y-%m-%d %H:%M:%S')
    op_code = f'''
    select a.Component, sum(a.McCabe) from Commit a
    inner join ( 
        select File, max(CommitterDate) as CommitterDate from Commit
        where CommitterDate < '{ lower_date_bound }' 
        group by File
    ) b on b.File = a.File and b.CommitterDate = a.CommitterDate
    where a.McCabe > 0
    group by a.Component;
    '''

    return run_op_code( project_name, op_code )

# cal process metrics function
def cal_change_entropy( project_name : str, release_date : datetime ) -> defaultdict:
    lower_date_bound = ( release_date - relativedelta( months=6 ) ).strftime( '%Y-%m-%d %H:%M:%S' )
    upper_date_bound = release_date.strftime('%Y-%m-%d %H:%M:%S')
    op_code = f'''
    select Component, ( Sum(p_k * log(2,p_k) * -1 ) / log(2,n) ) shanon_entropy from (
	select a.file, a.Component, file_change / comp_change as p_k, b.n from (
		select Component, File, Sum(Churn) as file_change from Commit
                where CommitterDate between '{ lower_date_bound }' and '{ upper_date_bound }'
		group by Component, File
	) a
	inner join (
		select Component, Sum(Churn) as comp_change, count(distinct(file)) as n from Commit
                where CommitterDate between '{ lower_date_bound }' and '{ upper_date_bound }'
		group by Component
	) b on a.Component = b.Component
    ) c
    group by Component
    having shanon_entropy is not null;
    '''

    return run_op_code( project_name, op_code )

def count_defect_fixed_prior_release( project_name : str, release_date : datetime ) -> defaultdict:
    lower_date_bound = ( release_date - relativedelta( months=6 ) ).strftime( '%Y-%m-%d %H:%M:%S' )
    upper_date_bound = release_date.strftime('%Y-%m-%d %H:%M:%S')
    op_code = f'''
    select Component, count(*) as bug_fixed from Commit
    where ChangeType = 'Fix' and CommitterDate between '{ lower_date_bound }' and '{ upper_date_bound }'
    group by Component
    '''

    return run_op_code( project_name, op_code )

def cal_churn( project_name : str, release_date : datetime ) -> defaultdict:
    lower_date_bound = ( release_date - relativedelta( months=6 ) ).strftime( '%Y-%m-%d %H:%M:%S' )
    upper_date_bound = release_date.strftime('%Y-%m-%d %H:%M:%S')
    op_code = f'''
    select Component, sum(Churn) from Commit
    where CommitterDate between '{ lower_date_bound }' and '{ upper_date_bound }'
    group by Component;
    '''

    return run_op_code( project_name, op_code )

# cal human factor 
def get_unique_author( project_name : str, release_date : datetime ) -> defaultdict:
    lower_date_bound = ( release_date - relativedelta( months=6 ) ).strftime( '%Y-%m-%d %H:%M:%S' )
    upper_date_bound = release_date.strftime('%Y-%m-%d %H:%M:%S')
    op_code = f'''
    select Component, count(distinct(AuthorName)) from Commit
    where CommitterDate between '{ lower_date_bound }' and '{ upper_date_bound }'
    group by Component;
    '''

    return run_op_code( project_name, op_code )

def get_minor_author( project_name : str, release_date : datetime ) -> defaultdict:
    lower_date_bound = ( release_date - relativedelta( months=6 ) ).strftime( '%Y-%m-%d %H:%M:%S' )
    upper_date_bound = release_date.strftime('%Y-%m-%d %H:%M:%S')
    op_code = f'''
    select Component, count(distinct(AuthorName)) from (
	select a.Component, a.AuthorName, author_commit / all_commit as contribute from (
		select Component, AuthorName, count(*) as author_commit from Commit
                where CommitterDate between '{ lower_date_bound }' and '{ upper_date_bound }'
		group by Component, AuthorName
	) a 
	inner join (
		select Component, count(*) as all_commit from Commit
                where CommitterDate between '{ lower_date_bound }' and '{ upper_date_bound }'
		group by Component
	) b on a.Component = b.Component
    ) c
    where contribute < 0.05
    group by Component;
    '''

    return run_op_code( project_name, op_code )

def get_major_author( project_name : str, release_date : datetime ) -> defaultdict:
    lower_date_bound = ( release_date - relativedelta( months=6 ) ).strftime( '%Y-%m-%d %H:%M:%S' )
    upper_date_bound = release_date.strftime('%Y-%m-%d %H:%M:%S')
    
    op_code = f'''
    select Component, count(distinct(AuthorName)) from (
	select a.Component, a.AuthorName, author_commit / all_commit as contribute from (
		select Component, AuthorName, count(*) as author_commit from Commit
                where CommitterDate between '{ lower_date_bound }' and '{ upper_date_bound }'
		group by Component, AuthorName
	) a 
	inner join (
		select Component, count(*) as all_commit from Commit
                where CommitterDate between '{ lower_date_bound }' and '{ upper_date_bound }'
		group by Component
	) b on a.Component = b.Component
    ) c
    where contribute >= 0.05
    group by Component;
    '''

    return run_op_code( project_name, op_code )

def cal_author_ownership( project_name : str, release_date : datetime ) -> defaultdict:
    lower_date_bound = ( release_date - relativedelta( months=6 ) ).strftime( '%Y-%m-%d %H:%M:%S' )
    upper_date_bound = release_date.strftime('%Y-%m-%d %H:%M:%S')
    op_code = f'''
    select a.Component, max(author_commit) / max(comp_commit) as owner_ship from (
	select Component, AuthorName, count(*) as author_commit from Commit
        where CommitterDate between '{ lower_date_bound }' and '{ upper_date_bound }'
	group by Component, AuthorName
    ) a
    inner join ( 
	select Component, count(*) as comp_commit from Commit
        where CommitterDate between '{ lower_date_bound }' and '{ upper_date_bound }'
        group by Component 
    ) b on a.Component = b.Component
    group by Component;
    '''

    return run_op_code( project_name, op_code )
    
def compute_review_change( review_name : str, project_name : str, release_date : datetime ):
    lower_date_bound = ( release_date - relativedelta( months=6 ) ).strftime( '%Y-%m-%d %H:%M:%S' )
    upper_date_bound = release_date.strftime('%Y-%m-%d %H:%M:%S')

    op_code = f'''
    select d.Component, commit_with_review / count(*) as review_cov from { project_name }.Commit d
    inner join (
	select Component, count(*) as commit_with_review from { project_name }.Commit a
	inner join ( 
		select GitRevision from { review_name }.Review
		inner join { review_name }.PatchSet on { review_name }.PatchSet.ReviewId = { review_name }.Review.ReviewId
                where { review_name }.Review.CreatedOn between '{ lower_date_bound }' and '{ upper_date_bound }'
	) b on b.GitRevision = a.GitRevision
	group by Component
    ) c on c.Component = d.Component
    where CommitterDate between '{ lower_date_bound }' and '{ upper_date_bound }'
    group by Component
    '''

    return run_op_code( project_name, op_code )

def compute_review_churn( review_name : str, project_name : str, release_date : datetime ):
    lower_date_bound = ( release_date - relativedelta( months=6 ) ).strftime( '%Y-%m-%d %H:%M:%S' )
    upper_date_bound = release_date.strftime('%Y-%m-%d %H:%M:%S')

    op_code = f'''
    select d.Component, churn_with_review / sum(Churn) as review_cov from KitwareCommits.Commit d
    inner join (
	select a.Component, sum(a.Churn) as churn_with_review from KitwareCommits.Commit a
	inner join ( 
		select GitRevision from KitwareReviews.Review
		inner join KitwareReviews.PatchSet on PatchSet.ReviewId = Review.ReviewId
	) b on b.GitRevision = a.GitRevision
	group by a.Component
    ) c on c.Component = d.Component
    where CommitterDate between '{ lower_date_bound }' and '{ upper_date_bound }'
    group by Component
    '''

    return run_op_code( project_name, op_code )

def count_self_approve( review_name : str, project_name : str, release_date : datetime ):
    lower_date_bound = ( release_date - relativedelta( months=6 ) ).strftime( '%Y-%m-%d %H:%M:%S' )
    upper_date_bound = release_date.strftime('%Y-%m-%d %H:%M:%S')

    op_code = f'''
    select commit.component, count(distinct(commit.GitRevision)) / max(comp_change) as self_approval  from { project_name }.Commit commit
    inner join ( 
	select person.Name as approval_name , patch.GitRevision from { review_name }.Person person
	inner join { review_name }.Approval approval on person.PersonId = approval.PersonId
	inner join { review_name }.PatchSet patch on patch.ReviewId = approval.ReviewId
    ) approval on approval.GitRevision = commit.GitRevision
    inner join (
	select component, count(distinct(commit.GitRevision)) as comp_change from { project_name }.Commit commit 
        group by component
    ) comp on comp.component = commit.component
    where approval.approval_name = commit.AuthorName and CommitterDate between '{ lower_date_bound }' and '{ upper_date_bound }'
    group by commit.component
    '''

    return run_op_code( project_name, op_code )

def count_hastily_review( review_name : str, project_name : str, release_date : datetime ):
    lower_date_bound = ( release_date - relativedelta( months=6 ) ).strftime( '%Y-%m-%d %H:%M:%S' )
    upper_date_bound = release_date.strftime('%Y-%m-%d %H:%M:%S')

    op_code = f'''
    select commit.component, count(distinct(commit.GitRevision)) / comp_change as hastily_review from { project_name }.Commit commit
    inner join { review_name }.PatchSet patch on patch.GitRevision = commit.GitRevision
    inner join ( 
        select component, count(distinct(GitRevision)) as comp_change from { project_name }.Commit commit
        group by component
        group by commit.Component 
    ) comp on comp.component = commit.component 
    where loc / ( time_to_sec(timediff(CreatedOn,AuthorDate)) / 3600 ) > 200 and commit.CommitterDate between '{ lower_date_bound }' and '{ upper_date_bound }'
    group by commit.Component
    '''

    return run_op_code( project_name, op_code )


def count_change_without_discuss( review_name : str, project_name : str, release_date : datetime ):
    lower_date_bound = ( release_date - relativedelta( months=6 ) ).strftime( '%Y-%m-%d %H:%M:%S' )
    upper_date_bound = release_date.strftime('%Y-%m-%d %H:%M:%S')

    op_code = f'''
    select comp.component, ( comp_count - count_change ) / comp_count as change_without_discuss from (
	select component, count(distinct(commit.GitRevision)) as count_change from { review_name }.Comment comment
	inner join { review_name }.PatchSet patch on patch.ReviewId = comment.ReviewId
	inner join { project_name }.Commit commit on commit.GitRevision = patch.GitRevision
        where commit.CommitterDate between '{ lower_date_bound }' and '{ upper_date_bound }'
	group by component
    ) change_with_discuss
    inner join (
	select component, count(distinct(commit.GitRevision)) as comp_count from { project_name }.Commit commit
        where commit.CommitterDate between '{ lower_date_bound }' and '{ upper_date_bound }'
	group by component
    ) comp on comp.component = change_with_discuss.component
    '''

    return run_op_code( project_name, op_code )

def cal_review_window( review_name : str, project_name : str, release_date : datetime ):
    lower_date_bound = ( release_date - relativedelta( months=6 ) ).strftime( '%Y-%m-%d %H:%M:%S' )
    upper_date_bound = release_date.strftime('%Y-%m-%d %H:%M:%S')

    op_code = f'''
    select Component, avg(time_length / Churn) as typical_review_window from { project_name }.Commit as commit
    inner join (
	select commit.GitRevision, time_to_sec(timediff(max(RatedOn),min(CreatedOn))) as time_length, sum(Churn) from { review_name }.Approval approval
	inner join { review_name }.PatchSet patch on patch.ReviewId = approval.ReviewId
	inner join { project_name }.Commit commit on commit.GitRevision = patch.GitRevision
	group by commit.GitRevision
    ) review_window on review_window.GitRevision = commit.GitRevision
    where commit.CommitterDate between '{ lower_date_bound }' and '{ upper_date_bound }'
    group by Component
    '''

    return run_op_code( project_name, op_code )

def cal_discussion_length( review_name : str, project_name : str, release_date : datetime ):
    lower_date_bound = ( release_date - relativedelta( months=6 ) ).strftime( '%Y-%m-%d %H:%M:%S' )
    upper_date_bound = release_date.strftime('%Y-%m-%d %H:%M:%S')

    op_code = f'''
    select Component, avg(comment_length / Churn ) as discuss_length from { project_name }.Commit commit
    inner join (
	select GitRevision, comment.ReviewId, char_length(comment.Message) as comment_length from { review_name }.Comment comment
	inner join { review_name }.Person person on comment.AuthorId = person.PersonId
	inner join { review_name }.PatchSet patch on patch.ReviewId = comment.ReviewId
	where person.IsBotAccount = 0
    ) comment_length_table on comment_length_table.GitRevision = commit.GitRevision
    where commit.CommitterDate between '{ lower_date_bound }' and '{ upper_date_bound }'
    group by Component
    '''

    return run_op_code( project_name, op_code )

def create_df( project_name : str, review_name : str, release_date : datetime ) -> pd.DataFrame:
    post_release_defect = count_post_release_defect( project_name, release_date )
    loc = count_loc( project_name, release_date )
    complex_score = cal_complexity( project_name, release_date )
    pre_release_defect = count_defect_fixed_prior_release( project_name, release_date )
    churn = cal_churn( project_name, release_date )
    change_entropy = cal_change_entropy( project_name, release_date )
    total_author = get_unique_author( project_name, release_date )
    minor_author = get_minor_author( project_name, release_date )
    major_author = get_major_author( project_name, release_date )
    author_ownership = cal_author_ownership( project_name, release_date )
    review_change = compute_review_change( review_name, project_name, release_date )
    review_churn = compute_review_churn( review_name, project_name, release_date )
    self_approval = count_self_approve( review_name, project_name, release_date )
    hastily_review = count_hastily_review( review_name, project_name, release_date )
    change_without_discuss = count_change_without_discuss( review_name, project_name, release_date )
    discuss_length = cal_discussion_length( review_name, project_name, release_date )
    # coverage metric

    
    comps = get_components( project_name, release_date )
    df = defaultdict(list)

    df['component'] = comps
    for comp in comps:
        df['post_release_defect'].append(post_release_defect[comp])
        df['loc'].append(loc[comp])
        df['complex_score'].append(complex_score[comp])
        df['pre_release_defect'].append(pre_release_defect[comp])
        df['churn'].append(churn[comp])
        df['change_entropy'].append(change_entropy[comp])
        df['total_author'].append(total_author[comp])
        df['minor_author'].append(minor_author[comp])
        df['major_author'].append(major_author[comp])
        df['author_ownership'].append(author_ownership[comp])
        df['review_change'].append( review_change[comp] )
        df['review_churn'].append( review_churn[comp] )
        df['self_approval'].append( self_approval[comp] )
        df['hastily_review'].append( hastily_review[comp] )
        df['change_without_discuss'].append( change_without_discuss[comp] )
        df['discuss_length'].append( discuss_length )

    return pd.DataFrame( df )

if __name__ == '__main__':
    project_data = {
        'QtCommits' : {
            'date' : datetime( 2012, 12, 19 ),
            'review' : 'QtReviews'
        },
        'KitwareCommits' : {
            'date' : datetime( 2012, 5, 14 ),
            'review' : 'KitwareReviews'
        }
    }

    for project_name in project_data.keys():
        print(f'start collect data from { project_name }')
        df = create_df( project_name, project_data[project_name]['review'], project_data[project_name]['date'] )
        df.to_csv( project_name + '.csv' )
        print(f'finish collect data from { project_name }')
