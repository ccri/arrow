function benchmark (method) {
    var start = +(new Date);

    method && method(function (callback) {
        var end = +(new Date);
        var difference = end - start;
        callback && callback(start, end, {
            milliseconds: difference,
            ms: difference,
            seconds: (difference / 1000) % 60,
            minutes: (difference / (1000 * 60)) % 60,
            hours: (difference / (1000 * 60 * 60)) % 24
        });
    });
};

function test1(a1,a2,i) { return a1[i] == 19; };
function test2(a1,a2,i) { return a2[i] == 8;  };
function test3(a1,a2,i) { return test1(a1,a2,i) && test2(a1,a2,i); };

var gdelt = reader.getVector('gdelt');

function withCalls() {
    var c = 0;
    var total = 0;
    reader.setBatchIndex(0);
    for(j=0; j<numBatches; j++) {
        var count = reader.loadNextBatch();
        var a1 = gdelt.getChildVectors()[7].dataView;
        var a2 = gdelt.getChildVectors()[8].dataView;
        for(i=0; i<count; i++) {
            total = total + 1;
            if(test3(a1,a2,i)) {
                c = c+1;
            }
        }
    }
}

function withoutCalls() {
    var c = 0;
    var total = 0;
    reader.setBatchIndex(0);
    for(j=0; j<numBatches; j++) {
        var count = reader.loadNextBatch();
        var a1 = gdelt.getChildVectors()[7].dataView;
        var a2 = gdelt.getChildVectors()[8].dataView;
        for(i=0; i<count; i++) {
            total = total + 1;
            if(a1[i] == 19 && a2[i] == 8) {
                c = c+1;
            }
        }
    }
}

benchmark(function (next) {
    withoutCalls();
    next(function (start, end, difference) {
        console.log('Processed in under: ' + difference.milliseconds + 's!');
    });
});

function doBench(m) {
    benchmark(function(next) {
        m();
        next(function (start, end, difference) {
            return difference.seconds;
        });
    });
};
