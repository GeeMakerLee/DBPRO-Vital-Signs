window.console = window.console || function(t) {};

// contains the patient objects of all patients
var patients = [];
// contains just the patient names
var patientNames = [];
// delete patient after x seconds without update
var deleteAfterX = 20;

// Ranges for the vital Signs
var hrmin = 50;
var hrmax = 100;
var abpmeanmin = 80;
var abpmeanmax = 90;
var papmeanmin = 21;
var papmeanmax = 24;
var cvpmin = 3;
var cvpmax = 8;
var pulsemin = 50;
var pulsemax = 100;
var respmin = 12;
var respmax = 20;
var spo2min = 95;
var spo2max = 100;
var nbpmeanmin = 90;
var nbpmeanmax = 110;


// constructor to create a new patient
function Patient(patientName) {
    this.name = patientName;
    this.date = [] // date
    this.HR = []; // HR
    this.ABPMean = []; // ABPMean
    this.PAPMean = []; // PAPMean
    this.CVP = []; // CVP
    this.Pulse = []; // Pulse
    this.Resp = []; // Resp 
    this.SpO2 = []; // SpO2
    this.NBPMean = []; // NBPMean
    this.chart; // chart
    this.timer = deleteAfterX; // remove timer
};

// handles incoming messages 
function webSocketInvoke() {
    if ("WebSocket" in window) {
        console.log("WebSocket is supported by your Browser!");
        var ws = new WebSocket("ws://localhost:8080/", "echo-protocol");

        ws.onopen = function() {
            console.log("Connection created");
        };

        ws.onmessage = function(evt) {

            // msg looks like: 2014-04-05 02:00:00,Person_G,63.61666666666669,86.63333333333334,0.0,11.191666666666666,63.55166666666667,19.96333333333333,96.30166666666668,0.0,0.0
            var msg = evt.data;
            var val = msg.split(",")

            // detect if patient is new and add to patients array, also create elements in DOM
            if (!patientNames.includes(val[1])) {
                console.log(msg)
                eval('var ' + val[1] + ' = new Patient("' + val[1] + '")');
                eval('patients.push(' + val[1] + ')');
                eval('patientNames.push(' + val[1] + '.name)');
                eval('createPatientContainer(' + val[1] + ')')
                    // creates chart and assigns it to the patient
                eval('createChart(' + val[1] + ')');
                // patient.timer gets counted down, once it hits 0 i deletes the chart from the DOM 
                eval('countdown(' + val[1] + ')');
            }
            // add values to corresponding patient
            for (let i = 0; i < patients.length; i++) {
                if (val[1].localeCompare(patients[i].name) == 0) {
                    var dateSplits = val[0].split(" ");
                    if (patients[i].date == []) {
                        patients[i].date.push(val[0]);
                    } else if (val[0].includes("00:00:00")) {
                        patients[i].date.push(dateSplits[0]);
                    } else {
                        patients[i].date.push(dateSplits[1]);
                    }
                    patients[i].HR.push(val[2]);
                    patients[i].ABPMean.push(val[3]);
                    patients[i].PAPMean.push(val[4]);
                    patients[i].CVP.push(val[5]);
                    patients[i].Pulse.push(val[6]);
                    patients[i].Resp.push(val[7]);
                    patients[i].SpO2.push(val[8]);
                    patients[i].NBPMean.push(val[9]);
                    // set deletion timer back to 20 sek 
                    patients[i].timer = deleteAfterX;
                    refresh(patients[i]);

                }
            }
        }

        ws.onclose = function() {
            console.log("Connection closed");
        };
    } else {
        alert("WebSocket NOT supported by your Browser!");
    }
};

// random int generator for randomized Age/Height/Weight
function getRndInteger(min, max) {
    return Math.floor(Math.random() * (max - min)) + min;
}
// countdown for deletion
var countdown = async function(patient) {
        if (patient.timer > 0) {
            patient.timer -= 1;
            await sleep(1000);
            countdown(patient);
        }
        if (patient.timer <= 0) {
            var ele = document.getElementById(patient.name);
            ele.parentNode.removeChild(ele);
            createNewRowInTable(patient.name, patient.date[patient.date.length - 1], "-", "-", "Patient has been deleted");

        }

    }
    // sleep function for countdown()
const sleep = (milliseconds) => {
        return new Promise(resolve => setTimeout(resolve, milliseconds))
    }
    // creates a chart and assigns it to the patients; searches for created html <canvas> tag of the patient and places the chart there
createChart = function(patient) {
    var ctx = document.getElementById(patient.name + "-canvas");
    var myChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: patient.date,
            datasets: [{
                    data: patient.HR,
                    label: "HR",
                    borderColor: "#FD3409",
                    fill: false
                },
                {
                    data: patient.NBPMean,
                    label: "NBPMean",
                    borderColor: "#721B1B",
                    fill: false
                },
                {
                    data: patient.PAPMean,
                    label: "PAPMean",
                    borderColor: "#ABFF03",
                    fill: false
                },
                {
                    data: patient.Pulse,
                    label: "Pulse",
                    borderColor: "#03FF53",
                    fill: false
                },
                {
                    data: patient.Resp,
                    label: "Resp",
                    borderColor: "#03FFE0",
                    fill: false
                },
                {
                    data: patient.SpO2,
                    label: "SpO2",
                    borderColor: "#4BC6FF",
                    fill: false
                },
                {
                    data: patient.CVP,
                    label: "CVP",
                    borderColor: "#E803FF",
                    fill: false
                },
                {
                    data: patient.ABPMean,
                    label: "ABPMean",
                    borderColor: "#9C9C9C",
                    fill: false
                }
            ]

        },
        options: {
            responsive: false
        }
    });
    patient.chart = myChart;
};

// refreshes the values and the chart displayed in the monitor by accessing the patients <div> element with the id equal to the patients name
refresh = function(patient) {
    patient.chart.update();
    var element = document.getElementById(patient.name);
    console.log()
    var hr = element.querySelector('.HR');
    updateVitalSignDisplay(hr, patient.HR, hrmax, hrmin, patient.name, patient.date[patient.date.length - 1], "HR");
    if (parseFloat(patient.HR[patient.HR.length - 1]).toFixed(2) != 0.00) {
        hr.textContent = parseFloat(patient.HR[patient.HR.length - 1]).toFixed(2) + " bpm";
    } else {
        hr.textContent = "-";
    }
    var abpmean = element.querySelector('.ABPMEAN');
    updateVitalSignDisplay(abpmean, patient.ABPMean, abpmeanmax, abpmeanmin, patient.name, patient.date[patient.date.length - 1], "ABPMEAN");
    if (parseFloat(patient.ABPMean[patient.ABPMean.length - 1]).toFixed(2) != 0.00) {
        abpmean.textContent = parseFloat(patient.ABPMean[patient.ABPMean.length - 1]).toFixed(2) + " mmHg";
    } else {
        abpmean.textContent = "-";
    }
    var papmean = element.querySelector('.PAPMEAN');
    updateVitalSignDisplay(papmean, patient.PAPMean, papmeanmax, papmeanmin, patient.name, patient.date[patient.date.length - 1], "PAPMEAN");
    if (parseFloat(patient.PAPMean[patient.PAPMean.length - 1]).toFixed(2) != 0.00) {
        papmean.textContent = parseFloat(patient.PAPMean[patient.PAPMean.length - 1]).toFixed(2) + " mmHg";
    } else {
        papmean.textContent = "-";
    }
    var cvp = element.querySelector('.CVP');
    updateVitalSignDisplay(cvp, patient.CVP, cvpmax, cvpmin, patient.name, patient.date[patient.date.length - 1], "CVP");
    if (parseFloat(patient.CVP[patient.CVP.length - 1]).toFixed(2) != 0.00) {
        cvp.textContent = parseFloat(patient.CVP[patient.CVP.length - 1]).toFixed(2) + " mmHg";
    } else {
        cvp.textContent = "-";
    }
    var pulse = element.querySelector('.PULSE');
    updateVitalSignDisplay(pulse, patient.Pulse, pulsemax, pulsemin, patient.name, patient.date[patient.date.length - 1], "PULSE");
    if (parseFloat(patient.Pulse[patient.Pulse.length - 1]).toFixed(2) != 0.00) {
        pulse.textContent = parseFloat(patient.Pulse[patient.Pulse.length - 1]).toFixed(2) + " bpm";
    } else {
        pulse.textContent = "-";
    }
    var resp = element.querySelector('.RESP');
    updateVitalSignDisplay(resp, patient.Resp, respmax, respmin, patient.name, patient.date[patient.date.length - 1], "RESP");
    if (parseFloat(patient.Resp[patient.Resp.length - 1]).toFixed(2) != 0.00) {
        resp.textContent = parseFloat(patient.Resp[patient.Resp.length - 1]).toFixed(2) + " breaths per min";
    } else {
        resp.textContent = "-";
    }
    var spo2 = element.querySelector('.SPO2');
    updateVitalSignDisplay(spo2, patient.SpO2, spo2max, spo2min, patient.name, patient.date[patient.date.length - 1], "SPO2");
    if (parseFloat(patient.SpO2[patient.SpO2.length - 1]).toFixed(2) != 0.00) {
        spo2.textContent = parseFloat(patient.SpO2[patient.SpO2.length - 1]).toFixed(2) + " %";
    } else {
        spo2.textContent = "-";
    }
    var nbpmean = element.querySelector('.NBPMEAN');
    updateVitalSignDisplay(nbpmean, patient.NBPMean, nbpmeanmax, nbpmeanmin, patient.name, patient.date[patient.date.length - 1], "NBPMEAN");
    if (parseFloat(patient.NBPMean[patient.NBPMean.length - 1]).toFixed(2) != 0.00) {
        nbpmean.textContent = parseFloat(patient.NBPMean[patient.NBPMean.length - 1]).toFixed(2) + " mmHg";
    } else {
        nbpmean.textContent = "-";
    }
    var date = element.querySelector('.DATE');
    date.textContent = patient.date[patient.date.length - 1];

};



// used to change the colors of the font in the monitor if above/below threshold 
function updateVitalSignDisplay(htmlElement, patientVitalSign, vitalSignMax, vitalSignMin, patientName, patientTime, vitalSignName) {
    if (parseFloat(patientVitalSign[patientVitalSign.length - 1]).toFixed(2) != 0.00) {
        if (parseFloat(patientVitalSign[patientVitalSign.length - 1]).toFixed(2) > vitalSignMax || parseFloat(patientVitalSign[patientVitalSign.length - 1]).toFixed(2) < vitalSignMin) {
            if (parseFloat(patientVitalSign[patientVitalSign.length - 1]).toFixed(2) > vitalSignMax) {
                if (parseFloat(patientVitalSign[patientVitalSign.length - 1]).toFixed(2) - vitalSignMax < 10) {
                    htmlElement.style.color = "orange";
                    createNewRowInTable(patientName, patientTime, vitalSignName, patientVitalSign[patientVitalSign.length - 1], "moderate");
                } else {
                    htmlElement.style.color = "red";
                    createNewRowInTable(patientName, patientTime, vitalSignName, patientVitalSign[patientVitalSign.length - 1], "critical");
                }
            }
            if (parseFloat(patientVitalSign[patientVitalSign.length - 1]).toFixed(2) < vitalSignMin) {
                if (vitalSignMin - parseFloat(patientVitalSign[patientVitalSign.length - 1]).toFixed(2) < 10) {
                    htmlElement.style.color = "orange";
                    createNewRowInTable(patientName, patientTime, vitalSignName, patientVitalSign[patientVitalSign.length - 1], "moderate");
                } else {
                    htmlElement.style.color = "red";
                    createNewRowInTable(patientName, patientTime, vitalSignName, patientVitalSign[patientVitalSign.length - 1], "critical");
                }
            }
        } else {
            htmlElement.style.color = "black";
        }
    } else {
        createNewRowInTable(patientName, patientTime, vitalSignName, patientVitalSign[patientVitalSign.length - 1], "no data received, please check connection");
        htmlElement.style.color = "black";
    }
}

// used to change tabs (sidebar)
function openTab(evt, tabName) {
    var i, x, tablinks;
    x = document.getElementsByClassName("tab");
    for (i = 0; i < x.length; i++) {
        x[i].style.display = "none";
    }
    tablinks = document.getElementsByClassName("tablink");
    for (i = 0; i < x.length; i++) {
        tablinks[i].className = tablinks[i].className.replace(" w3-red", "");
    }
    document.getElementById(tabName).style.display = "block";
    evt.currentTarget.className += " w3-red";
}

// creates new row in alert table 
function createNewRowInTable(name, time, vitalSign, value, alertLevel) {
    var element = document.getElementById("tablebody");

    var tr = document.createElement("tr");

    element.insertBefore(tr, element.firstChild);

    var td1 = document.createElement("td");
    td1.textContent = name;

    tr.appendChild(td1);

    var td2 = document.createElement("td");
    td2.textContent = time;

    tr.appendChild(td2);

    var td3 = document.createElement("td");
    td3.textContent = vitalSign;

    tr.appendChild(td3);

    var td4 = document.createElement("td");
    if (parseFloat(value).toFixed(2) != 0.00) {
        td4.textContent = parseFloat(value).toFixed(2);
    } else {
        td4.textContent = "-";
    }


    tr.appendChild(td4);

    var td5 = document.createElement("td");
    td5.textContent = alertLevel;

    tr.appendChild(td5);
}

var input = document.getElementById("searchBar");
input.addEventListener("input", myFunction);

function myFunction(e) {
    var filter = e.target.value.toUpperCase();

    var patientContainer = document.getElementById("patients-container");
    var divs = patientContainer.childNodes;
    for (var i = 0; i < divs.length; i++) {
        var a = divs[i].getElementsByTagName("a")[0];

        if (a) {
            if (a.innerHTML.toUpperCase().indexOf(filter) > -1) {
                divs[i].style.display = "";
            } else {
                divs[i].style.display = "none";
            }
        }
    }

}

// creates the patients html objects
function    (patient) {
    var element = document.getElementById("patients-container");

    var div0 = document.createElement("div");
    div0.setAttribute("id", patient.name);

    element.appendChild(div0);

    var div = document.createElement("div");
    div.setAttribute("class", "col-lg row");

    div0.appendChild(div);

    var div2 = document.createElement("div");
    div2.setAttribute("class", "col-lg-4");

    div.appendChild(div2);

    var div3 = document.createElement("div");
    div3.setAttribute("class", "iq-card iq-user-profile-block");

    div2.appendChild(div3);

    var div4 = document.createElement("div");
    div4.setAttribute("class", "iq-card iq-user-profile-block");

    div3.appendChild(div4);

    var div5 = document.createElement("div");
    div5.setAttribute("class", "user-details-block");

    div4.appendChild(div5);

    var div6 = document.createElement("div");
    div6.setAttribute("class", "user-profile text-center");

    div5.appendChild(div6);

    var img = document.createElement("img");
    img.setAttribute("src", "images.jpg");
    img.setAttribute("alt", "patient-img");
    img.setAttribute("class", "avatar-130 img-fluid");

    div6.appendChild(img);

    var div7 = document.createElement("div");
    div7.setAttribute("class", "text-center mt-3");

    div5.appendChild(div7);

    var h4 = document.createElement("h4");
    h4.setAttribute("class", "bold");
    h4.textContent = patient.name

    div7.appendChild(h4);

    var ul = document.createElement("ul");
    ul.setAttribute("class", "doctoe-sedual d-flex align-items-center justify-content-between p-0 mt-4 mb-0");

    div5.appendChild(ul);

    var li = document.createElement("li");
    li.setAttribute("class", "text-center");

    ul.appendChild(li);

    var h3 = document.createElement("h3");
    h3.setAttribute("class", "text-primary");
    h3.textContent = "Weight";

    li.appendChild(h3);

    var h1 = document.createElement("h1");
    h1.textContent = getRndInteger(55, 120) + " kg";

    li.appendChild(h1);

    var li2 = document.createElement("li");
    li2.setAttribute("class", "text-center");

    ul.appendChild(li2);

    var h32 = document.createElement("h3");
    h32.setAttribute("class", "text-primary");
    h32.textContent = "Height";

    li2.appendChild(h32);

    var h12 = document.createElement("h1");
    h12.textContent = getRndInteger(150, 202) + " cm";

    li2.appendChild(h12);

    var li3 = document.createElement("li");
    li3.setAttribute("class", "text-center");

    ul.appendChild(li3);

    var h33 = document.createElement("h3");
    h33.setAttribute("class", "text-primary");
    h33.textContent = "Age";

    li3.appendChild(h33);

    var h13 = document.createElement("h1");
    h13.textContent = "" + getRndInteger(22, 80);

    li3.appendChild(h13);

    var div8 = document.createElement("div");
    div8.setAttribute("class", "col-lg-8");

    div.appendChild(div8);

    var div9 = document.createElement("div");
    div9.setAttribute("class", "iq-card");

    div8.appendChild(div9);

    var div10 = document.createElement("div");
    div10.setAttribute("class", "iq-card-body");

    div9.appendChild(div10);

    var div11 = document.createElement("div");
    div11.setAttribute("class", "user-details-block");

    div10.appendChild(div11);

    var div12 = document.createElement("div");
    div12.setAttribute("class", "text-center mt-3");

    div11.appendChild(div12);

    var h34 = document.createElement("h3");
    h34.setAttribute("class", "bold");
    h34.textContent = "Time and date";

    div12.appendChild(h34);

    var p = document.createElement("p");
    p.setAttribute("class", "DATE");
    p.textContent = patient.date[patient.date.length - 1] + "";

    div12.appendChild(p);

    var ul2 = document.createElement("ul");
    ul2.setAttribute("class", "doctoe-sedual d-flex align-items-center justify-content-between p-0 mt-4 mb-0");

    div11.appendChild(ul2);

    var li4 = document.createElement("li");
    li4.setAttribute("class", "text-center");

    ul2.appendChild(li4);

    var h35 = document.createElement("h3");
    h35.setAttribute("class", "text-primary");
    h35.textContent = "HR";

    li4.appendChild(h35);

    var h14 = document.createElement("h1");
    h14.setAttribute("class", "HR")
    h14.textContent = parseFloat(patient.HR[patient.HR.length - 1]).toFixed(2) + " bpm";

    li4.appendChild(h14);

    var li5 = document.createElement("li");
    li5.setAttribute("class", "text-center");

    ul2.appendChild(li5);

    var h36 = document.createElement("h3");
    h36.setAttribute("class", "text-primary");
    h36.textContent = "ABPMean";

    li5.appendChild(h36);

    var h15 = document.createElement("h1");
    h15.setAttribute("class", "ABPMEAN");
    h15.textContent = parseFloat(patient.ABPMean[patient.ABPMean.length - 1]).toFixed(2) + " mmHg";

    li5.appendChild(h15);

    var li6 = document.createElement("li");
    li6.setAttribute("class", "text-center");

    ul2.appendChild(li6);

    var h37 = document.createElement("h3");
    h37.setAttribute("class", "text-primary");
    h37.textContent = "PAPMean";

    li6.appendChild(h37);

    var h16 = document.createElement("h1");
    h16.setAttribute("class", "PAPMEAN");
    h16.textContent = parseFloat(patient.PAPMean[patient.PAPMean.length - 1]).toFixed(2) + " mmHg";

    li6.appendChild(h16);

    var li7 = document.createElement("li");
    li7.setAttribute("class", "text-center");

    ul2.appendChild(li7);

    var h38 = document.createElement("h3");
    h38.setAttribute("class", "text-primary");
    h38.textContent = "NBPMean";

    li7.appendChild(h38);

    var h17 = document.createElement("h1");
    h17.setAttribute("class", "NBPMEAN");
    h17.textContent = parseFloat(patient.NBPMean[patient.NBPMean.length - 1]).toFixed(2) + " mmHg";

    li7.appendChild(h17);

    var ul3 = document.createElement("ul");
    ul3.setAttribute("class", "doctoe-sedual d-flex align-items-center justify-content-between p-0 mt-4 mb-0");

    div11.appendChild(ul3);

    var li8 = document.createElement("li");
    li8.setAttribute("class", "text-center");

    ul3.appendChild(li8);

    var h39 = document.createElement("h3");
    h39.setAttribute("class", "text-primary");
    h39.textContent = "Pulse";

    li8.appendChild(h39);

    var h18 = document.createElement("h1");
    h18.setAttribute("class", "PULSE");
    h18.textContent = parseFloat(patient.Pulse[patient.Pulse.length - 1]).toFixed(2) + " bpm";

    li8.appendChild(h18);

    var li9 = document.createElement("li");
    li9.setAttribute("class", "text-center");

    ul3.appendChild(li9);

    var h310 = document.createElement("h3");
    h310.setAttribute("class", "text-primary");
    h310.textContent = "Resp";

    li9.appendChild(h310);

    var h19 = document.createElement("h1");
    h19.setAttribute("class", "RESP");
    h19.textContent = parseFloat(patient.Resp[patient.Resp.length - 1]).toFixed(2) + " breaths per minute";

    li9.appendChild(h19);

    var li10 = document.createElement("li");
    li10.setAttribute("class", "text-center");

    ul3.appendChild(li10);

    var h311 = document.createElement("h3");
    h311.setAttribute("class", "text-primary");
    h311.textContent = "SpO2";

    li10.appendChild(h311);

    var h110 = document.createElement("h1");
    h110.setAttribute("class", "SPO2");
    h110.textContent = parseFloat(patient.SpO2[patient.SpO2.length - 1]).toFixed(2) + " %";

    li10.appendChild(h110);

    var li11 = document.createElement("li");
    li11.setAttribute("class", "text-center");

    ul3.appendChild(li11);

    var h312 = document.createElement("h3");
    h312.setAttribute("class", "text-primary");
    h312.textContent = "CVP";

    li11.appendChild(h312);

    var h111 = document.createElement("h1");
    h111.setAttribute("class", "CVP");
    h111.textContent = parseFloat(patient.CVP[patient.CVP.length - 1]).toFixed(2) + " mmHg";

    li11.appendChild(h111);

    var button = document.createElement("button");
    button.setAttribute("class", "accordion");
    button.textContent = "show/hide chart";
    button.addEventListener("click", function() {
        this.classList.toggle("active");
        var panel = this.nextElementSibling;
        if (panel.style.display === "block") {
            panel.style.display = "none";
        } else {
            panel.style.display = "block";
        }
    });

    div0.appendChild(button);

    var div13 = document.createElement("div");
    div13.setAttribute("class", "panel");

    div0.appendChild(div13);

    var can = document.createElement("canvas");
    can.setAttribute("id", patient.name + "-canvas");
    can.setAttribute("Width", "1800")
    can.setAttribute("height", "400")


    div13.appendChild(can);

}

webSocketInvoke();