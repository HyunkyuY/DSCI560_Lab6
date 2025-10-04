const map = L.map('map').setView([47.5, -103.5], 6);
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  maxZoom: 19,
  attribution: '&copy; OpenStreetMap contributors'
}).addTo(map);

fetch('/api/wells').then(r=>r.json()).then(js=>{
  const layer = L.geoJSON(js, {
    pointToLayer: (feat, latlng) => L.marker(latlng),
    onEachFeature: (feat, layer) => {
      const raw = feat.properties || {};
      // friendly labels
      const LABELS = {
        well_name_number: 'Well', api_number: 'API', operator_company: 'Operator', address: 'Address',
        date_stimulated: 'Date', stimulated_formation: 'Formation', top_ft: 'Top (ft)', bottom_ft: 'Bottom (ft)',
        stimulation_stages: 'Stages', volume_value: 'Volume', volume_units: 'Units', treatment_type: 'Treatment',
        acid_percent: 'Acid %', lbs_proppant: 'Lbs Proppant', max_treatment_pressure_psi: 'Max Pressure (psi)',
        max_treatment_rate_bbls_per_min: 'Max Rate (bbls/min)', details: 'Details'
      };

      // filter/clean values (remove obvious header-like garbage)
      function cleanVal(v){
        if (v === null || v === undefined) return '';
        v = String(v).trim();
        if (!v) return '';
        const low = v.toLowerCase();
        // skip generic placeholders
        const garbage = ['telephone number','city state zip code','state izp code','state izip code','i state zip code'];
        for (const g of garbage) if (low.includes(g)) return '';
        // trim repeated header-like long blocks
        if (v.length > 800) return v.slice(0,800) + '...';
        return v;
      }

      // build popup
      const keys = Object.keys(LABELS);
      let title = cleanVal(raw.well_name_number) || cleanVal(raw.api_number) || 'Unnamed';
      let html = '<div>';
      html += `<h3>${title}</h3>`;
      html += '<table>';
      for (const k of keys) {
        const val = cleanVal(raw[k]);
        if (val) {
          html += `<tr><th style="text-align:left;padding:2px 8px;">${LABELS[k]}</th><td style="padding:2px 8px;">${val}</td></tr>`;
        }
      }
      // also include any other non-empty properties not in LABELS (in case)
      for (const k of Object.keys(raw)){
        if (keys.indexOf(k) !== -1) continue;
        const val = cleanVal(raw[k]);
        if (val) html += `<tr><th style="text-align:left;padding:2px 8px;">${k}</th><td style="padding:2px 8px;">${val}</td></tr>`;
      }
      html += '</table>';
      html += '</div>';
      layer.bindPopup(html,{maxWidth:400});
    }
  }).addTo(map);

  // If we have features, fit map to their bounds
  try {
    const bounds = layer.getBounds();
    if (bounds.isValid()) {
      map.fitBounds(bounds.pad(0.1));
    }
  } catch (e) {
    console.warn('Could not fit bounds:', e);
  }

  // Add a small control showing count of markers
  const count = (js.features || []).length;
  const info = L.control({position: 'topright'});
  info.onAdd = function () {
    const div = L.DomUtil.create('div', 'map-count-control');
    div.style.padding = '6px 8px';
    div.style.background = 'rgba(255,255,255,0.9)';
    div.style.borderRadius = '4px';
    div.style.boxShadow = '0 1px 2px rgba(0,0,0,0.2)';
    div.innerHTML = `<strong>Wells:</strong> ${count}`;
    return div;
  };
  info.addTo(map);
}).catch(e=>{console.error('failed to load wells',e); alert('Failed to load wells: '+e)});
