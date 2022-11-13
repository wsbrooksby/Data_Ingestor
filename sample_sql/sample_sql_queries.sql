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
Get the number of artists in each genre
*/
select
    genre.`name` as genre_name,
	count(genre_artist.artist_id) as artist_count
from genre
join genre_artist
	on genre.genre_id = genre_artist.genre_id
group by genre.genre_id
order by genre.`name`;