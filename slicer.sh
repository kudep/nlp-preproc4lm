for i in {1..200} ; 
do 
mongo --quiet --host mongo vect --eval "db.news.find({},{'cleaned_description':1,'_id': 0, 'banned':1, 'title':1}).skip($(( (i - 1)*10000 ))).limit(10000).toArray()"  > slices/slice_${i}.json 
done