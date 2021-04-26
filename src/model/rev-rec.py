import json

def read_raw_json( f_name : str ) -> dict :
    with open( f_name ) as f:
        return json.load(f)

def compute_fpath_sim( fpath1 : str, fpath2 : str ) -> dict:
    res = {
        'LCP' : 0.0,
        'LCS' : 0.0,
        'LCSubstr' : 0.0,
        'LCSubseq' : 0.0
    }

    fpath1_comp = fpath1.split('/') if len(fpath1) <= len(fpath2) else fpath2.split('/')
    fpath2_comp = fpath1.split('/') if len(fpath1) > len(fpath2) else fpath2.split('/')

    for i in range( len(fpath1_comp) ):
        if fpath1_comp[i] != fpath2_comp[i]:
            res['LCP'] = i / len(fpath2_comp)
            break

    for i in range( 1, len(fpath1_comp)+1 ):
        if fpath1_comp[-i] != fpath2_comp[-i]:
            res['LCS'] =  ( i - 1 ) / len(fpath2_comp)
            break

    lcs = [[ 0 for i in range(len(fpath1_comp) + 1) ] for j in range(len(fpath2_comp)+1) ]
    result = 0
    for i in range( len(fpath2_comp) + 1 ):
        for j in range( len(fpath1_comp) + 1 ):
            if i == 0 or j == 0:
                lcs[i][j] == 0
            if fpath2_comp[i-1] == fpath1_comp[j-1]:
                lcs[i][j] = lcs[i-1][j-1] + 1
                result = max( result, lcs[i][j] )
            else:
                lcs[i][j] = 0
    res['LCSubstr'] = result / len(fpath2_comp)

    lcs = [[ 0 for i in range(len(fpath2_comp) + 1) ] for j in range(len(fpath1_comp) + 1) ]
    for i in range(len(fpath1_comp) + 1):
        for j in range(len(fpath2_comp) + 1):
            if i == 0 or j == 0 :
                lcs[i][j] = 0
            elif fpath1_comp[i-1] == fpath2_comp[j-1]:
                lcs[i][j] = lcs[i-1][j-1]+1
            else:
                lcs[i][j] = max(lcs[i-1][j], lcs[i][j-1])
    res['LCSubseq'] = lcs[-1][-1] / len(fpath2_comp)

    return res
        
def compute_candidates_scores( past_reviews : list, cur_review : dict ):
    candidates = dict()

    for review in past_reviews:
        score = {
            'LCP' : 0.0,
            'LCS' : 0.0,
            'LCSubstr' : 0.0,
            'LCSubseq' : 0.0
        }

        for patch_filepath in review['filePaths']:
            for cur_filepath in cur_review['filePaths']:
                fpath_sim = compute_fpath_sim( patch_filepath['location'], cur_filepath['location'] )
                score['LCP'] += fpath_sim['LCP']
                score['LCS'] += fpath_sim['LCS']
                score['LCSubstr'] += fpath_sim['LCSubstr']
                score['LCSubseq'] += fpath_sim['LCSubseq']

        if score['LCSubseq'] == 0:
            continue 

        for score_type in score.keys():
            score[score_type] /= ( len( review['filePaths'] ) * len( cur_review['filePaths'] ) )

        for reviewer in review['reviewers']:
            if reviewer['accountId'] not in candidates.keys():
                candidates[reviewer['accountId']] = score
            else:
                candidate = candidates[reviewer['accountId']]
                candidate['LCP'] += score['LCP']
                candidate['LCS'] += score['LCS']
                candidate['LCSubstr'] += score['LCSubstr']
                candidate['LCSubseq'] += score['LCSubseq']

    return candidates

def rank_candidate( candidates_score : dict ):
    lcp_ranks = [ item[0] for item in sorted( candidates_score.items(), key=lambda item : item[1]['LCP'], reverse=True ) if item[1]['LCP'] != 0 ]
    lcs_ranks = [ item[0] for item in sorted( candidates_score.items(), key=lambda item : item[1]['LCS'], reverse= True ) if item[1]['LCS'] != 0 ]
    lcsubstr_ranks = [ item[0] for item in sorted( candidates_score.items(), key=lambda item : item[1]['LCSubstr'], reverse= True ) if item[1]['LCSubstr'] != 0 ]
    lcsubseq_ranks = [ item[0] for item in sorted( candidates_score.items(), key=lambda item : item[1]['LCSubseq'], reverse= True ) if item[1]['LCSubseq'] != 0 ]

    comb_scores = dict()
    for idx, candidate in enumerate(lcp_ranks):
        comb_score = len(lcp_ranks) - idx
        if candidate in lcs_ranks:
            comb_score += len(lcs_ranks) - lcs_ranks.index(candidate)
        if candidate in lcsubstr_ranks:
            comb_score += len(lcsubstr_ranks) - lcsubstr_ranks.index(candidate)
        if candidate in lcsubseq_ranks:
            comb_score += len(lcsubseq_ranks) - lcsubseq_ranks.index(candidate)
        comb_scores[candidate] = comb_score

    return [ item[0] for item in sorted( comb_scores.items(), key = lambda item : item[1] ) ]
        
def top_k_accuracy( k : int, candidates : list, reviews : list ):
    score = 0
    for idx, review in enumerate(reviews):
        for reviewer in review['reviewers']:
            if reviewer['accountId'] in candidates[idx][:k]:
                score += 1
    return score / len(reviews)

def model( data_path : str ):
    data = read_raw_json( data_path )
    sorted_reviews = sorted( data, key=lambda item : item['timestamp'] )

    candidates = list()
    for idx, review in enumerate(sorted_reviews[1:], start=1):
        candidates_score = compute_candidates_scores( sorted_reviews[:idx], review )
        candidate = rank_candidate(candidates_score)

        candidates.append( candidate )

    print( 'top_k_accuracy' )
    print(f'k = 1 : { top_k_accuracy( 1, candidates, sorted_reviews ) }')
    print(f'k = 3 : { top_k_accuracy( 3, candidates, sorted_reviews ) }')
    print(f'k = 5 : { top_k_accuracy( 5, candidates, sorted_reviews ) }')
    print(f'k = 10 : { top_k_accuracy( 10, candidates, sorted_reviews ) }')

if __name__ == '__main__':
    model('../../rev-rec-data/android.json')
