var reportInterval;
var queue = 'q1';

function init() {
	console.log('started v4');
	showReport();
	reportInterval = setInterval(showReport, 5000);
	var insjob = $('.insert-job');
	$('.ok', insjob).click(() => insjob.data('onOk')());
	$('.cancel', insjob).click(() => insjob.data('onCancel')());
	$('.insert-job').hide();
}
var data;

async function showReport() {
	var txt = await $.get('../report').promise();
	data = JSON.parse(txt);
	var tbody = $('.workers tbody');
	tbody.empty();
	for (var i = 0; i < data.workers.length; i++) {
		let d = data.workers[i];
		let job = d.job || { operation: '-', key1: '-', key2: '-', key3: '-', lock: '-', priority: '-', nretry: '-' };
		let div = $('<div/>').addClass('job-desc').appendTo('.workers');
		var row = $(`<tr>
			<td class="tag">${d.tag}</td>
			<td class="priority">${job.priority}</td>
			<td class="nretry">${job.nretry}</td>
			<td class="operation">${job.operation}</td>
			<td class="par1">${job.key1}</td>
			<td class="par2">${job.key2}</td>
			<td class="par3">${job.key3}</td>
			<td class="lock">${d.status}</td>
		</tr>`).appendTo(tbody);
		row.attr('title', JSON.stringify(d, null, 2));
	}

	$('.db').empty();
	var nitems = "#job=" + data.job[0].count
		+ ", #done=" + data.done[0].count
		+ ", #error=" + data.error[0].count;

	$('<b style="margin-right:20px">elementi in tabella: </b>').appendTo('.db.count');
	$('<span class="n-items"/>').text(nitems).appendTo('.db.count');

	$('<b style="margin-right:20px">by operation: </b>').appendTo('.db.operation');
	data.operation
		.map(x => x.operation + ": " + x.count)
		.map(txt => $('<div/>').text(txt).appendTo('.db.operation'));

	$('<b style="margin-right:20px">by status: </b>').appendTo('.db.status');
	data.status
		.map(x => x.queue + ':' + x.locktag + ": " + x.count)
		.map(txt => $('<div/>').text(txt).appendTo('.db.status'));

	$('.connections').empty();
	$('<b style="margin-right:20px">connessioni DB: </b>').appendTo('.connections');

	data.connections.map(txt => $('<div/>').text(txt).appendTo('.connections'))
}

function exit() {
	$.get('exit').then(() => alert("done!"));

}

function setN() {
	var n = prompt("quanti workers?");
	if (n === null)
		return;
	n = n - 0;
	console.log("setn " + n);
	$.get('setn/' + n).then(() => console.log("set n=" + n + ", done!"));
}

async function getListeDbs() {
	var os = "PDM";

	var path = "/dbs/lavori";
	var folderId = "{A1B84F73-3F41-4BC7-843B-C21EBD1A1829}";
	launch(folderId, "lavori"); 
	var path = "/dbs/progetti";
	var folderId = "{CD703774-E2DB-494C-98CE-6B8618E6A761}";
	launch(folderId, "progetti"); 


	function launch(folderId, tag) {
		let job = {
			queue,
			operation: 'getListaDbs',
			priority: 100,
			key1: tag,
			key2: folderId,
			key3: tag,
			os, 
			folderId,
			maxrecursion: 100, 
			withdoc: 0, 
			withcontent: 0 
		};
		$.post({url:'insertJob', data:JSON.stringify(job)}).promise().then(
			ok => console.log(JSON.stringify(JSON.parse(ok), null, 2)),
			error => alert(error)
		);
	}
}

// async function getProgettoSingolo() {
// 	var os = "PDM";

// 	var path = "/dbs/progetti/LO.710.E.D.19.01";
// 	var folderId = "{B35E558C-8317-4260-985B-C2B93B7B8A5A}";
// 	let job = {
// 		queue: 'anas-etl',
// 		operation: 'getFolderMD',
// 		priority: 10000,
// 		key1: os,
// 		key2: path,
// 		key3: "0",
// 		os, 
// 		folderId,
// 		maxrecursion: 100, 
// 		withdoc: 1, 
// 		withcontent: 0 

// 	};
// 	$.post({url:'insertJob', data:JSON.stringify(job)}).promise().then(
// 		ok => console.log(JSON.stringify(JSON.parse(ok), null, 2)),
// 		error => alert(error)
// 	);
	
// }


function startScanArchivi() {
	var withdoc = confirm("with doc");
	var withcontent = withdoc && confirm("with content");

	let job = {
		queue,
		operation: 'startScanArchivi',
		priority:1000000,
		withdoc, 
		withcontent,
		buildDir: false
	};

	var msg = JSON.stringify(job, null, 4);
	if (!confirm(msg))
		return;

	$.post({url:'insertJob', data:JSON.stringify(job)}).promise().then(
		ok => alert(JSON.stringify(JSON.parse(ok), null, 2)),
		error => alert(error)
	);
}

function startScanProgetti() { startScanDBS('progetti'); }
function startScanLavori() { startScanDBS('lavori'); }

function startScanDBS(what) {
	var withdoc = confirm("with doc");
	var withcontent = withdoc && confirm("with content");

	let job = {
		queue,
		operation: 'startScanDBS',
		priority:2000000,
		key1: what,
		withdoc, 
		withcontent,
		buildDir: true
	};

	var msg = JSON.stringify(job, null, 4);
	if (!confirm(msg))
		return;

	$.post({url:'insertJob', data:JSON.stringify(job)}).promise().then(
		ok => alert(JSON.stringify(JSON.parse(ok), null, 2)),
		error => alert(error)
	);
}

async function sql(query) {
	$.post({url:'sql', data:query}).promise().then(
		ok => console.log(ok),
		error => alert(error)
	);
}


$(init);