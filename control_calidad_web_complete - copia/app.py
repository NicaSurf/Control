from flask import Flask, render_template, request, redirect, send_file, session, url_for, flash
import sqlite3
import pandas as pd
from datetime import datetime, date
from functools import wraps

app = Flask(__name__)
app.secret_key = 'CAMBIA_POR_ALGO_SEGURO'

DB_NAME = 'control_calidad.db'

USERS = {
    'roger@incostas.com':  '19734268',
    'ashley@incostas.com': '19734268',
    'martin@incostas.com': '19734268'
}
USER_ROLES = {
    'roger@incostas.com':  'control_de_calidad',
    'ashley@incostas.com': 'vaciado',
    'martin@incostas.com': 'admin'
}

connected_users = set()

def login_required(f):
    @wraps(f)
    def w(*a,**k):
        if 'user' not in session:
            flash('Inicia sesión.', 'warning')
            return redirect(url_for('login'))
        return f(*a,**k)
    return w

def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS camiones (
          id INTEGER PRIMARY KEY,
          fecha TEXT, camion_num TEXT, placa TEXT,
          volumen REAL, boleta TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS controles (
          id INTEGER PRIMARY KEY, id_camion INTEGER,
          punto_control TEXT, usuario TEXT,
          hora_control TEXT, temperatura REAL,
          revenimiento TEXT, estado TEXT,
          repeticion INTEGER, vinculada_a INTEGER,
          cilindros_geonic INTEGER, cilindros_incostas INTEGER,
          observaciones TEXT, hora_salida TEXT,
          hora_diseno REAL, hora_vaciado TEXT,
          FOREIGN KEY(id_camion) REFERENCES camiones(id)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS messages (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user TEXT, message TEXT, timestamp TEXT
        )
    """)
    conn.commit(); conn.close()

@app.route('/login', methods=['GET','POST'])
def login():
    if request.method=='POST':
        e=request.form['email'].strip().lower(); p=request.form['password']
        if e in USERS and USERS[e]==p:
            session['user'], session['role'] = e, USER_ROLES[e]
            connected_users.add(e)
            flash(f'Bienvenido {e}','success')
            return redirect(url_for('index'))
        flash('Credenciales inválidas.','danger')
    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    connected_users.discard(session['user'])
    session.clear()
    flash('Sesión cerrada.','info')
    return redirect(url_for('login'))

@app.route('/chat', methods=['POST'])
@login_required
def chat():
    m=request.form['message'].strip()
    if m:
        t=datetime.now().strftime('%H:%M')
        conn=sqlite3.connect(DB_NAME)
        conn.execute("INSERT INTO messages(user,message,timestamp) VALUES(?,?,?)",
                     (session['user'],m,t))
        conn.commit(); conn.close()
    return redirect(request.referrer or url_for('index'))

@app.route('/')
@login_required
def index():
    conn=sqlite3.connect(DB_NAME)
    df= pd.read_sql_query("""
      SELECT c.camion_num,c.boleta, c.volumen,
             ctrl.punto_control,ctrl.usuario,
             ctrl.hora_control,ctrl.hora_salida,
             ctrl.hora_diseno,ctrl.hora_vaciado
      FROM controles ctrl
      JOIN camiones c ON ctrl.id_camion=c.id
      ORDER BY ctrl.id DESC LIMIT 10
    """, conn)
    total= conn.execute("SELECT COALESCE(SUM(volumen),0) FROM camiones").fetchone()[0]
    msgs=pd.read_sql_query("SELECT * FROM messages ORDER BY id DESC LIMIT 20",conn)
    conn.close()
    return render_template('index.html',
      user=session['user'], role=session['role'],
      historial=df.to_dict('records'),
      total_volume=total,
      connected=sorted(connected_users),
      messages=msgs.to_dict('records')
    )

@app.route('/registrar', methods=['POST'])
@login_required
def registrar():
    d=request.form; conn=sqlite3.connect(DB_NAME);c=conn.cursor()
    c.execute("INSERT INTO camiones(fecha,camion_num,placa,volumen,boleta) VALUES(?,?,?,?,?)",
              (d['fecha'],d['camion_num'],d['placa'],float(d['volumen']),d['boleta']))
    cid=c.lastrowid; role=session['role']
    hs=d.get('hora_salida'); hd=float(d.get('hora_diseno')) if role=='control_de_calidad' else None
    hv=d.get('hora_vaciado') if role=='vaciado' else None
    c.execute("""INSERT INTO controles
      (id_camion,punto_control,usuario,hora_control,temperatura,
       revenimiento,estado,repeticion,vinculada_a,cilindros_geonic,
       cilindros_incostas,observaciones,hora_salida,hora_diseno,hora_vaciado)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",(
      cid,d['punto_control'],session['user'],d['hora_control'],float(d['temperatura']),
      d['revenimiento'],d['estado'],1 if d.get('repeticion') else 0,
      int(d.get('vinculada_a') or 0),int(d.get('cilindros_geonic') or 0),
      int(d.get('cilindros_incostas') or 0),d.get('observaciones',''),
      hs,hd,hv
    ))
    conn.commit(); conn.close()
    return redirect(url_for('index'))

@app.route('/mensajes', methods=['GET','POST'])
@login_required
def mensajes():
    usuario = session.get('user')
    rol = session.get('role')
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    if request.method == 'POST':
        msg = request.form['message']
        t = datetime.now().strftime('%Y-%m-%d %H:%M')
        c.execute("INSERT INTO messages(user,message,timestamp) VALUES(?,?,?)", (usuario, msg, t))
        conn.commit()
    df = pd.read_sql_query("SELECT * FROM messages ORDER BY id DESC LIMIT 50", conn)
    conn.close()
    return render_template('mensajes.html', messages=df.to_dict('records'), usuario=usuario, rol=rol)

@app.route('/historial')
@login_required
def historial():
    usuario = session.get('user')
    rol = session.get('role')
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql_query("""
        SELECT c.fecha, c.camion_num, c.placa, c.volumen, c.boleta,
               ctrl.punto_control, ctrl.usuario, ctrl.hora_control,
               ctrl.temperatura, ctrl.revenimiento, ctrl.estado,
               ctrl.repeticion, ctrl.cilindros_geonic, ctrl.cilindros_incostas,
               ctrl.observaciones, ctrl.hora_salida, ctrl.hora_diseno, ctrl.hora_vaciado
        FROM controles ctrl
        JOIN camiones c ON ctrl.id_camion = c.id
        ORDER BY ctrl.id DESC
    """, conn)
    conn.close()
    return render_template('historial.html', datos=df.to_dict('records'), usuario=usuario, rol=rol)


@app.route('/exportar')
@login_required
def exportar():
    conn=sqlite3.connect(DB_NAME)
    q="""SELECT c.fecha,c.camion_num,c.placa,c.volumen,c.boleta,
         ctrl.punto_control,ctrl.usuario,ctrl.hora_control,
         ctrl.temperatura,ctrl.revenimiento,ctrl.estado,
         ctrl.repeticion,ctrl.cilindros_geonic,ctrl.cilindros_incostas,
         ctrl.observaciones,ctrl.hora_salida,ctrl.hora_diseno,ctrl.hora_vaciado
         FROM controles ctrl JOIN camiones c ON ctrl.id_camion=c.id"""
    p=()
    if session['role']=='control_de_calidad':
        q+=" WHERE ctrl.usuario=?"; p=(session['user'],)
    df=pd.read_sql_query(q,conn,params=p); conn.close()
    fn=f"reporte_{session['user']}_{date.today()}.xlsx"
    df.to_excel(fn,index=False); return send_file(fn,as_attachment=True)

@app.route('/vaciado', methods=['GET','POST'])
@login_required
def vaciado():
    if session['role'] not in('vaciado','admin'):
        flash('Acceso denegado.','danger'); return redirect(url_for('index'))
    conn=sqlite3.connect(DB_NAME)
    if request.method=='POST':
        cid=int(request.form['control_id']); hv=request.form['hora_vaciado']
        conn.execute("UPDATE controles SET hora_vaciado=? WHERE id=?", (hv,cid))
        conn.commit(); flash(f'Vaciado a las {hv}','success')
    df=pd.read_sql_query("""
      SELECT ctrl.id,c.camion_num,c.boleta,c.volumen,
             ctrl.hora_salida,ctrl.hora_control,ctrl.hora_vaciado
      FROM controles ctrl JOIN camiones c ON ctrl.id_camion=c.id
      ORDER BY ctrl.id DESC LIMIT 10""",conn)
    conn.close()
    historial=[]; today=date.today()
    for r in df.to_dict('records'):
        dpqc=dsv=dcv=None
        if r['hora_salida'] and r['hora_control']:
            hs=datetime.combine(today,datetime.strptime(r['hora_salida'],'%H:%M').time())
            hc=datetime.combine(today,datetime.strptime(r['hora_control'],'%H:%M').time())
            dpqc=str(hc-hs)
        if r['hora_vaciado']:
            hv_dt=datetime.combine(today,datetime.strptime(r['hora_vaciado'],'%H:%M').time())
            if r['hora_salida']:
                hs=datetime.combine(today,datetime.strptime(r['hora_salida'],'%H:%M').time())
                dsv=str(hv_dt-hs)
            if r['hora_control']:
                hc=datetime.combine(today,datetime.strptime(r['hora_control'],'%H:%M').time())
                dcv=str(hv_dt-hc)
        row=r.copy(); row.update(delta_planta_qc=dpqc,delta_salida_vaciado=dsv,delta_control_vaciado=dcv)
        historial.append(row)
    return render_template('vaciado.html',
      user=session['user'],role=session['role'],
      historial=historial,
      connected=sorted(connected_users),
      messages=pd.read_sql_query("SELECT * FROM messages ORDER BY id DESC LIMIT 20",sqlite3.connect(DB_NAME)).to_dict('records')
    )

if __name__=='__main__':
    init_db(); app.run(debug=True)
