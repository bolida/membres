from app import db, Session, SessionMinistere, SessionMinistereMembre, Role

session_id = 2  # id de la nouvelle session
role_default = Role.query.filter(Role.nom.ilike("membre")).first()

if role_default:
    smm_sans_role = (
        SessionMinistereMembre.query
        .join(SessionMinistere, SessionMinistere.id == SessionMinistereMembre.session_ministere_id)
        .filter(SessionMinistere.session_id == session_id)
        .filter(SessionMinistereMembre.role_id.is_(None))
        .all()
    )

    for smm in smm_sans_role:
        smm.role_id = role_default.id

    db.session.commit()
