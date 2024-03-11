//Autor: Francisco Adrian Guasumba Tupiza
//Base de datos Spotify coleccion Spotify

use Spotify
show collections

//Exploracion de base de datos en general

db.Spotify.find()

//Inserts

db.Spotify.insertMany([
  {
    track_name: "Cancion1",
    "artist(s)_name": "Ejemplo",
    artist_count: 1,
    released_year: 2023,
    released_month: 3,
    released_day: 15,
    in_spotify_playlists: 10,
    in_spotify_charts: 5,
    streams: 500000,
    in_apple_playlists: 5,
    in_apple_charts: 10,
    in_deezer_playlists: 3,
    in_deezer_charts: 15,
    in_shazam_charts: 20,
    bpm: 130,
    key: "G",
    mode: "Major",
    danceability_percentage: 75,
    valence_percentage: 85,
    energy_percentage: 88,
    acousticness_percentage: 8,
    instrumentalness_percentage: 7,
    liveness_percentage: 12,
    speechiness_percentage: 6
  },
  {
    track_name: "Cancion2",
    "artist(s)_name": "Ejemplo",
    artist_count: 1,
    released_year: 2023,
    released_month: 3,
    released_day: 16,
    in_spotify_playlists: 12,
    in_spotify_charts: 8,
    streams: 600000,
    in_apple_playlists: 6,
    in_apple_charts: 12,
    in_deezer_playlists: 4,
    in_deezer_charts: 18,
    in_shazam_charts: 25,
    bpm: 135,
    key: "A",
    mode: "Minor",
    danceability_percentage: 80,
    valence_percentage: 90,
    energy_percentage: 85,
    acousticness_percentage: 10,
    instrumentalness_percentage: 5,
    liveness_percentage: 15,
    speechiness_percentage: 5
  },
  {
    track_name: "Cancion3",
    "artist(s)_name": "Ejemplo",
    artist_count: 1,
    released_year: 2023,
    released_month: 3,
    released_day: 17,
    in_spotify_playlists: 15,
    in_spotify_charts: 10,
    streams: 700000,
    in_apple_playlists: 7,
    in_apple_charts: 15,
    in_deezer_playlists: 5,
    in_deezer_charts: 20,
    in_shazam_charts: 30,
    bpm: 140,
    key: "C",
    mode: "Major",
    danceability_percentage: 70,
    valence_percentage: 80,
    energy_percentage: 90,
    acousticness_percentage: 5,
    instrumentalness_percentage: 8,
    liveness_percentage: 10,
    speechiness_percentage: 7
  }
]);
// vista de nuevos campos 

//db.Spotify.find( {"artist(s)_name": "Ejemplo"})

///// OPERACIONES ACTUALIZACION

db.Spotify.find()
//UPDATE 1
db.Spotify.find({
  $or: [ { "artist(s)_name": { $regex: /^Z.*[ï-]/ } },
    { "track_name": { $regex: /^Z.[-ï]/ } } 
    ]});

db.Spotify.updateOne(
  { "track_name": "Malvada" },
  { $set: { "artist(s)_name": "Ze Neto & Crist" } }
);
db.Spotify.updateOne(
  { "track_name": "Oi Balde - Ao Vivo" },
  { $set: { "artist(s)_name": "Ze Felipe" } }
);
db.Spotify.find({ "track_name": { $in: ["Malvada", "Oi Balde - Ao Vivo"] } });

 //Proyecciones 1
db.Spotify.find()
db.Spotify.find({}, { "artist(s)_name":1,"in_spotify_playlists":1, _id:0}).sort({"in_spotify_playlists":-1});
 
//CONSULTAS

// Top 10 canciones
 
db.Spotify.find().sort({ "streams": -1 }).limit(10);

 // Comparacion de musicas por estaciones
 db.Spotify.find(
  {
    $or: [
      { released_month: 12 },
      { released_month: 7 }
    ]
  },
  {_id: 0, track_name: 1,"artist(s)_name": 1,"released_month": 1,"danceability_%":1 ,"energy_%": 1,"speechiness_%": 1,"acousticness_%": 1
  }
);

 // Comparacion de musicas por estaciones media de metricas
 
 db.Spotify.aggregate([
    {
        $match: {
            $or: [
                { released_month: 12 },{ released_month: 7 }
            ]
        }
    },
    {
        $group: {
            _id: "$released_month",
            avg_danceability: { $avg: "$danceability_%" },
            avg_energy: { $avg: "$energy_%" },
            avg_speechiness: { $avg: "$speechiness_%" },
            avg_acousticness: { $avg: "$acousticness_%" }
        }
    },
    {
        $project: {
            _id: 0,
            released_month: "$_id",
            avg_danceability: 1,
            avg_energy: 1,
            avg_speechiness: 1,
            avg_acousticness: 1
        }
    }
]);

// Tonalidad mas usada en el dataset 

db.Spotify.aggregate([
    { $group: {_id: "$key",total: { $sum: 1 }} },
    {$project: { _id: 0,key: "$_id",total: 1}},
    { $sort: {total: -1}}
])

// Comparacion de tonalidad 

db.Spotify.aggregate([
  { $match: { $or: [{ key: "C#" }, { key: "D#" }] } },
  { $group: { _id: "$key", Media_reproducciones : { $avg: "$streams" } } },
  { $project: { _id: 0, key: "$_id", Media_reproducciones: { $round: ["$Media_reproducciones", 2] } } }
])

// Update canciones sin datos

db.Spotify.updateMany(
   { "key": null },   
   { $set: { "key": "Sin dato" } }  
)

//Maximo y minimo bpm 

var project =  { $project: { _id: 0,cancion: "$track_name", artista: "$artist(s)_name", bpm: 1 } } 
db.Spotify.aggregate([
  { $sort: { bpm: -1 } },  { $limit: 1 },project       
])
var project =  { $project: { _id: 0,cancion: "$track_name", artista: "$artist(s)_name", bpm: 1 } } 
db.Spotify.aggregate([
  { $sort: { bpm: 1 } },  { $limit: 1 },     project      
])


// Analisis de distribucion de Bpm 

db.Spotify.aggregate([
  {
    $bucket: {
      groupBy: "$bpm",
      boundaries: [0, 80, 120, 360],default: "Otros",
      output: {contador: { $sum: 1 } }
    }
  },
  {
    $project: {
      _id: 0,contador: 1
     intervalo: {
        $switch: {
          branches: [
            { case: { $and: [ { $gte: ["$_id", 0] }, { $lt: ["$_id", 80] } ] }, then: "Bajo" },
            { case: { $and: [ { $gte: ["$_id", 80] }, { $lt: ["$_id", 120] } ] }, then: "Medio" },
            { case: { $gte: ["$_id", 120] }, then: "Alto" },], default: "Otros"
                }
      }}
  },
  {
    $group: {_id: "$intervalo",total: { $sum: "$contador" }}
  }
])

// Plataforma popular 

db.Spotify.aggregate([
  { $group: {
      _id: null,
      SpotyList: { $sum: "$in_spotify_playlists" },
      AppleList: { $sum: "$in_apple_playlists" },
      DeezerList: { $sum: "$in_deezer_playlists" }}
  },
  { $project: {
      _id: 0,
      platform: ["Spotify", "Apple Music", "Deezer"],
      Numero_Listas: ["$SpotyList", "$AppleList", "$DeezerList"] }
  }
])


//Maximo y minimo lanzamiento año

var project =  { $project: { _id: 0,cancion: "$track_name", artista: "$artist(s)_name", released_year: 1 ,  reproducciones: "$streams" } } 
db.Spotify.aggregate([
  { $sort: { released_year: -1 } },  { $limit: 10 },project       
])
var project =  { $project: { _id: 0,cancion: "$track_name", artista: "$artist(s)_name", released_year: 1 ,  reproducciones: "$streams" } } 
db.Spotify.aggregate([
  { $sort: { released_year: 1 } },  { $limit: 10 },     project      
])

//Analisis canciones antiguas y recientes

db.Spotify.aggregate([
  { $group: {
      _id: { $cond: { if: { $lt: ["$released_year", 2000] }, then: "Antiguas", else: "Recientes" } },
      count: { $sum: 1 },
      Reproducciones: { $avg: "$streams" }
  } }
])

// Analisis de top reproducciones 

var project =  { $project: { _id: 0,cancion: "$track_name", artista: "$artist(s)_name", released_year: 1 ,  reproducciones: "$streams" } } 
db.Spotify.aggregate([
  { $match: { released_year: { $lt: 2000 } } },
  { $sort: { streams: -1 } },
  { $limit: 10 },project
])
var project =  { $project: { _id: 0,cancion: "$track_name", artista: "$artist(s)_name", released_year: 1 ,  reproducciones: "$streams" } } 

db.Spotify.aggregate([
  { $match: { released_year: { $gt: 2000 } } },
  { $sort: { streams: -1 } },
  { $limit: 10 },project
])


