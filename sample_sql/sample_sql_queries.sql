/*
Get names and Ids for artists that are not actual artists (like "Various Artists")
*/
select `name`, artist_id
from artist
where is_actual_artist = 0;


/*
Get artists by the parent genre of their main genre
*/
select
	parent_g.`name` as parent_genre,
    primary_g.`name` as primary_genre,
	artist.`name` as artist_name,
    artist.view_url as url
from artist
join genre_artist as genre_artist
	on artist.artist_id = genre_artist.artist_id
    and genre_artist.is_primary = 1
join genre as primary_g
	on genre_artist.genre_id = primary_g.genre_id
join genre as parent_g
	on primary_g.parent_id = parent_g.genre_id
where artist.is_actual_artist = 1
order by parent_g.`name`, primary_g.`name`, artist.`name`;


/*
Get the number of artists in each genre, only using genres that have artists.
	- There are some duplicate records in the data, and the ETL script should be updated to find and remove them when populating the database.
*/
select
    genre.`name` as genre_name,
	count(distinct(genre_artist.artist_id)) as artist_count
from genre
join genre_artist
	on genre.genre_id = genre_artist.genre_id
group by genre.`name`
order by genre.`name`;


/*
Get the average number of genres for artists who have at least one genre
*/
with cte as (
	select
		a.`name` as artist_name,
        gc.genre_count as genre_count
	from artist as a
        left join (
			select artist_id, count(artist_id) as genre_count 
            from genre_artist 
            group by artist_id) as gc on gc.artist_id = a.artist_id
    )
select 
	avg(cte.genre_count) as avg_number_of_genres
from cte;


/*
Get cases where a genre name shows up with different genre ids.
*/
with cte2 as (
	select
        g.genre_id,
        g.`name` as genre_name,
		row_number() over(partition by g.`name`) as num
	from genre as g
)
select 
	genre_name,
    max(num) as number_of_duplicates,
	group_concat(distinct genre_id) as genre_id_list
from cte2
group by genre_name
having number_of_duplicates > 1
order by genre_name;


/*
Show full parentage for all genres that have parents.
	- The fully qualified parentage would likely need to be used for many queries that indentify individual genres, since genre names are not unique and the associated parentage varies.
*/
with recursive cte3 as (
    select 
		genre_id,
        `name`,
        parent_id, 
        1 lvl 
	from genre
    union all
    select 
		c.genre_id, 
        g.`name`,
        g.parent_id, 
        lvl + 1
    from cte3 as c
		join genre as g on g.genre_id = c.parent_id
)
select 
	genre_id, 
    `name` as genre_name,
    count(parent_id) as number_of_parents,
    group_concat(`name` order by lvl separator " --> ") as parentage
from cte3
group by genre_id
having parentage is not null
order by genre_name;