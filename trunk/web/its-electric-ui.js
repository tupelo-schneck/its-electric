/*
This file is part of
"it's electric": software for storing and viewing home energy monitoring data
Copyright (C) 2009--2012 Robert R. Tupelo-Schneck <schneck@gmail.com>
http://tupelo-schneck.org/its-electric

"it's electric" is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

"it's electric" is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with "it's electric", as legal/COPYING-agpl.txt.
If not, see <http://www.gnu.org/licenses/>.
*/

function show(id) {
    var e = document.getElementById(id);
    e.style.display = '';
}
function hide(id) {
    var e = document.getElementById(id);
    e.style.display = 'none';
}

function setMin(id) {
    var val = document.getElementById(id).value;
    if(val && val!='') itsElectric.options.min = val;
    else delete itsElectric.options.min;
    if(itsElectric.queryPath=='/voltage' || (val && val!='')) itsElectric.options.scaleType = 'maximized';
    else itsElectric.options.scaleType = 'fixed';
}
function setMax(id) {
    var val = document.getElementById(id).value;
    if(val && val!='') itsElectric.options.max = val;
    else delete itsElectric.options.max;
}

function changeView(view) {
    itsElectric.queryPath = "/" + view;
    if(view=="voltage") {
        hide('wattsMinAndMax');
        show('voltsMinAndMax');
        itsElectric.options.allValuesSuffix="V";
        setMin('vmin');
        setMax('vmax');
    }
    else if(view=="power-factor") {
        hide('wattsMinAndMax');
        hide('voltsMinAndMax');
        delete itsElectric.options.allValuesSuffix;
        itsElectric.options.scaleType = 'fixed';
        delete itsElectric.options.min;
        delete itsElectric.options.max;
    }
    else {
        show('wattsMinAndMax');
        hide('voltsMinAndMax');
        if(view=="volt-amperes") itsElectric.options.allValuesSuffix="VA";
        else if(view=="volt-amperes-reactive") itsElectric.options.allValuesSuffix="var";
        else itsElectric.options.allValuesSuffix="W";
        setMin('wmin');
        setMax('wmax');
    }
    itsElectric.requery();
}            

window.onload = function(){
    itsElectric.delta = document.getElementById('delta').checked;
    changeView(document.getElementById('view').value);
    if(!itsElectric.hasVoltage) {
        document.getElementById('view.voltage').disabled = true;
    }
    if(!itsElectric.hasKVA) {
        document.getElementById('view.volt-amperes').disabled = true;
        document.getElementById('view.volt-amperes-reactive').disabled = true;
        document.getElementById('view.combined-power').disabled = true;
        document.getElementById('view.power-factor').disabled = true;
    }
    itsElectric.redraw();
};

function errorCallback() {
	if(itsElectric.getErrors().length > 0) document.getElementById('errorsLink').style.display = '';
	else document.getElementById('errorsLink').style.display = 'none';
	if(document.getElementById('errorsOverlay').style.visibility == 'visible') {
		hideErrors();
		showErrors();
	}
}

function showErrors() {
    var errorsDiv = document.getElementById('errorsDiv');
    while(errorsDiv.firstChild) errorsDiv.removeChild(errorsDiv.firstChild);
    errorsDiv.appendChild(htmlListify(itsElectric.getErrors()));
    document.getElementById('errorsOverlay').style.visibility = 'visible';
}
function clearErrors() {
	itsElectric.clearErrors();
    hideErrors();
    errorCallback();
}
function hideErrors() {
    document.getElementById('errorsOverlay').style.visibility = 'hidden';
}

function htmlListify(xs) {
    var ol = document.createElement("ol");
    for(var i = 0; i < xs.length; i++) {
        var li = document.createElement("li");
        li.appendChild(document.createTextNode(xs[i]));
        ol.appendChild(li);
    }
    return ol;
}
