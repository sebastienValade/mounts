import utilityme as utils

# dbo = utils.Database(db_host='127.0.0.1', db_usr='root', db_pwd='wave', db_type='mysql')
# stmt = "SELECT * FROM DB_MOUNTS.results_img WHERE target_id = '221080'"


db_name = 'DB_MOUNTS'
dbo = utils.Database(db_host='127.0.0.1', db_usr='root', db_pwd='wave', db_type='mysql', db_name=db_name)
stmt = "SELECT r.title FROM results_img r INNER JOIN archive a ON r.id_master = a.id WHERE r.target_id = '221080' ORDER BY a.acqstarttime desc"

# stmt = '''
#     SELECT R.title, A.acqstarttime
#     FROM results_img AS R
#     INNER JOIN archive AS A
#     ON R.id_master = A.id
#     WHERE R.target_id = '221080' AND R.type = 'ifg' OR R.type = 'coh'
#     ORDER BY A.acqstarttime desc
#     '''

id = '22180'

stmt = '''
    SELECT R.title, A.acqstarttime
    FROM results_img AS R
    INNER JOIN archive AS A
    ON R.id_master = A.id
    WHERE R.target_id = {} AND R.type = 'ifg' OR R.type = 'coh'
    ORDER BY A.acqstarttime desc
    '''

stmt = stmt.format(88)

# --- using GROUP BY statement
# stmt = '''
#     SELECT type, COUNT(*) AS "groups_type"
#     FROM results_img
#     WHERE target_id = '221080'
#     GROUP BY type
#     '''

# stmt = '''
#     SELECT type, COUNT(*) AS "groups_type"
#     FROM results_img AS R
#     INNER JOIN archive AS A
#     ON R.id_master = A.id
#     WHERE R.target_id = '221080'
#     GROUP BY R.type
#     '''

res = dbo.execute_query(stmt)

print(res.dataset)
print ""


print(res.as_dict())
print ""


from collections import defaultdict
res_sorted = defaultdict(list)
for i in res:
    print i
    res_sorted[i['acqstarttime']].append(i)

print res_sorted
print ""
res_sorted = dict(res_sorted)

from collections import OrderedDict
res_sorted = OrderedDict(sorted(res_sorted.items()))
print res_sorted
print type(res_sorted)


# # --- sort results by master_id
# from collections import defaultdict
# res_sorted = defaultdict(list)
# for i in res:
#     res_sorted[i['id_master']].append(i)
# res_sorted = dict(res_sorted)

# print(res_sorted)


# for r in res:
#     abspath = r.abspath
#     print abspath
