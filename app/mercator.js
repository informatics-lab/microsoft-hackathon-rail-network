
// Borrowed from a blog post
// http://dotnetfollower.com/wordpress/2011/08/javascript-how-to-convert-latitude-and-longitude-to-mercator-coordinates/
export function toMercator({ latitude, longitude }) {
    var rMajor = 6378137; //Equatorial Radius, WGS84
    var shift  = Math.PI * rMajor;
    var x      = longitude * shift / 180;
    var y      = Math.log(Math.tan((90 + latitude) * Math.PI / 360)) / (Math.PI / 180);
    y = y * shift / 180;
    return {x, y};
}

