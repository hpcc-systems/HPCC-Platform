const gulp = require('gulp')
const filter = require('gulp-filter');
const jshint = require('gulp-jshint');

const lintFilter = filter(["**"]);

gulp.task('lint', function () {
    return gulp.src('./eclwatch/**/*.js')
        .pipe(lintFilter)
        .pipe(jshint('.jshintrc'))
        .pipe(jshint.reporter('jshint-stylish'))
        .pipe(jshint.reporter('fail'))
    ;
});
