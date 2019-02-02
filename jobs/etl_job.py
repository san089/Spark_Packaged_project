from Utils.spark_session import create_session

def clean_data(spark):
    df = spark.read.csv("C:/Users/Sanchit Kumar/Downloads/youtubedata.txt", sep="\t", header=False, inferSchema=True)
    df = df.select(df.columns[:9])
    df.persist()
    df = df.toDF('video_id', 'uploader', 'Interval', 'Category', 'Length', 'Views', 'Rating', 'Num_rating',
                 'Num_comment')
    return df

def first_job(df):
    category_grp = df.groupby('Category').count()
    result1 = category_grp.orderBy('count', ascending=False).take(5)
    print("Top 5 Categories (Category name and count): \n")
    for category, count in result1:
        print('{} : {}'.format(category, count))
    return None

def second_job(df):
    result2 = df.select(['video_id', 'Rating']).orderBy('Rating', ascending=False).take(10)
    print("Top 10 video by rating (video_id and rating): \n")
    for video_id, rating in result2:
        print('{} : {}'.format(video_id, rating))
    return None

if __name__ == "__main__":
    spark = create_session()
    df = clean_data(spark)
    first_job(df)
    second_job(df)
