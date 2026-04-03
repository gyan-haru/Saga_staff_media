from __future__ import annotations

import os
from functools import wraps
from flask import Flask, abort, render_template, request, Response, redirect, url_for

from database import (
    create_person_for_mention,
    fetch_departments,
    fetch_department_profiles,
    fetch_network_snapshot,
    fetch_pending_identity_reviews,
    fetch_pending_reviews,
    fetch_people,
    fetch_person_detail,
    fetch_policy_topic_index,
    fetch_project_detail,
    fetch_projects,
    fetch_staff_roster,
    init_db,
    link_person_mention_to_timeline_recommendation,
    link_person_mention_to_person,
    mark_person_mention_distinct,
    merge_departments,
    merge_people,
    group_department_profiles_by_top_unit,
    update_project_review,
    fetch_interviews,
    save_interview,
    fetch_interview,
    fetch_department_detail,
)

app = Flask(__name__)

def check_admin_auth(username, password):
    admin_user = os.environ.get("ADMIN_USER", "admin")
    admin_pass = os.environ.get("ADMIN_PASS", "admin123")
    return username == admin_user and password == admin_pass

def authenticate():
    return Response(
        'Could not verify your access level for that URL.\n'
        'You have to login with proper credentials', 401,
        {'WWW-Authenticate': 'Basic realm="Login Required"'})

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_admin_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated


@app.route("/")
def index():
    source_type = request.args.get("source_type") or None
    projects = fetch_projects(limit=200, source_type=source_type)
    people = fetch_people(limit=20)
    interviews = fetch_interviews()
    return render_template("index.html", projects=projects, people=people, source_type=source_type, interviews=interviews)


@app.route("/projects")
def projects():
    source_type = request.args.get("source_type") or None
    rows = fetch_projects(limit=500, source_type=source_type)
    return render_template("projects.html", projects=rows, source_type=source_type)


@app.route("/projects/<int:project_id>")
def project_detail(project_id: int):
    project = fetch_project_detail(project_id)
    if not project:
        abort(404)
    return render_template("project_detail.html", project=project)


@app.route("/people")
def people():
    rows = fetch_people(limit=500)
    return render_template("people.html", people=rows)


@app.route("/directory")
def staff_directory():
    payload = fetch_staff_roster()
    return render_template("roster.html", **payload)


@app.route("/departments")
def departments():
    rows = fetch_department_profiles()
    groups = group_department_profiles_by_top_unit(rows)
    return render_template("departments.html", departments=rows, department_groups=groups)


@app.route("/departments/<int:department_id>")
def department_detail(department_id: int):
    payload = fetch_department_detail(department_id)
    if not payload:
        abort(404)
    return render_template("department_detail.html", **payload)


@app.route("/network")
def network():
    selected_top_unit = (request.args.get("top_unit") or "").strip() or None
    payload = fetch_network_snapshot(top_unit_filter=selected_top_unit)
    return render_template("network.html", **payload)


@app.route("/topics")
def topics():
    rows = fetch_policy_topic_index()
    return render_template("topics.html", topics=rows)


@app.route("/people/<person_key>")
def person_detail(person_key: str):
    payload = fetch_person_detail(person_key)
    if not payload:
        abort(404)
    return render_template("person_detail.html", **payload)


@app.route("/admin")
@requires_auth
def admin_index():
    projects = fetch_projects(limit=50) # Just as an overview
    return render_template("admin/index.html", projects=projects)

@app.route("/admin/projects")
@requires_auth
def admin_projects():
    projects = fetch_pending_reviews(limit=100)
    return render_template("admin/projects.html", projects=projects)

@app.route("/admin/projects/<int:project_id>", methods=["GET", "POST"])
@requires_auth
def admin_project_detail(project_id: int):
    if request.method == "POST":
        action = request.form.get("action", "save") # 'save', 'approve', 'reject'
        status_map = {"save": "pending", "approve": "approved", "reject": "rejected"}
        review_status = status_map.get(action, "pending")
        
        update_project_review(
            project_id=project_id,
            review_status=review_status,
            title=request.form.get("title", ""),
            summary=request.form.get("summary", ""),
            purpose=request.form.get("purpose", ""),
            budget=request.form.get("budget", ""),
            app_deadline=request.form.get("application_deadline", ""),
            sub_deadline=request.form.get("submission_deadline", ""),
            department_name=request.form.get("department_name", ""),
            person_name=request.form.get("person_name", ""),
            person_role=request.form.get("role", "")
        )
        return redirect(url_for("admin_projects"))

    project = fetch_project_detail(project_id)
    if not project:
        abort(404)
    return render_template("admin/project_review.html", project=project)

@app.route("/admin/people", methods=["GET", "POST"])
@requires_auth
def admin_people():
    if request.method == "POST":
        action = request.form.get("action")
        if action == "merge":
            primary_id = request.form.get("primary_id", type=int)
            secondary_id = request.form.get("secondary_id", type=int)
            if primary_id and secondary_id and primary_id != secondary_id:
                merge_people(primary_id, secondary_id)
        return redirect(url_for("admin_people"))
        
    people = fetch_people(limit=500)
    return render_template("admin/people.html", people=people)


@app.route("/admin/identities", methods=["GET", "POST"])
@requires_auth
def admin_identities():
    if request.method == "POST":
        action = request.form.get("action")
        mention_id = request.form.get("mention_id", type=int)
        if mention_id:
            if action == "link":
                person_id = request.form.get("person_id", type=int)
                if person_id:
                    link_person_mention_to_person(mention_id, person_id)
            elif action == "timeline_link":
                employee_slot_id = request.form.get("employee_slot_id", type=int)
                if employee_slot_id:
                    link_person_mention_to_timeline_recommendation(mention_id, employee_slot_id)
            elif action == "create":
                create_person_for_mention(mention_id)
            elif action == "distinct":
                mark_person_mention_distinct(mention_id, request.form.get("notes", ""))
        return redirect(url_for("admin_identities"))

    reviews = fetch_pending_identity_reviews(limit=100)
    return render_template("admin/identities.html", reviews=reviews)

@app.route("/admin/departments", methods=["GET", "POST"])
@requires_auth
def admin_departments():
    if request.method == "POST":
        action = request.form.get("action")
        if action == "merge":
            primary_id = request.form.get("primary_id", type=int)
            secondary_id = request.form.get("secondary_id", type=int)
            if primary_id and secondary_id and primary_id != secondary_id:
                merge_departments(primary_id, secondary_id)
        return redirect(url_for("admin_departments"))
        
    departments = fetch_departments()
    return render_template("admin/departments.html", departments=departments)


@app.route("/admin/interviews", methods=["GET", "POST"])
@requires_auth
def admin_interviews():
    if request.method == "POST":
        person_id = request.form.get("person_id", type=int)
        project_id = request.form.get("project_id", type=int)
        title = request.form.get("title", "")
        content = request.form.get("content", "")
        if person_id and title and content:
            save_interview(person_id, title, content, project_id)
        return redirect(url_for("admin_interviews"))
    
    interviews = fetch_interviews()
    people = fetch_people(limit=500)
    projects = fetch_projects(limit=500)
    return render_template("admin/interviews.html", interviews=interviews, people=people, projects=projects)


if __name__ == "__main__":
    init_db()
    app.run(debug=True, host="0.0.0.0", port=8000)
